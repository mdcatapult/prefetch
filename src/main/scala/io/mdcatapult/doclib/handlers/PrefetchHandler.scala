package io.mdcatapult.doclib.handlers

import java.io.{File, FileInputStream, FileNotFoundException}
import java.nio.file.attribute.BasicFileAttributeView
import java.nio.file.{Files, Path, Paths, StandardCopyOption}
import java.time.{LocalDateTime, ZoneOffset}

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import better.files._
import cats.data._
import cats.implicits._
import com.mongodb.client.result.UpdateResult
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import io.lemonlabs.uri.Uri
import io.mdcatapult.doclib.messages.{DoclibMsg, PrefetchMsg}
import io.mdcatapult.doclib.models.metadata._
import io.mdcatapult.doclib.models.{Derivative, DoclibDoc, FileAttrs, Origin}
import io.mdcatapult.doclib.remote.{DownloadResult, UndefinedSchemeException, Client => RemoteClient}
import io.mdcatapult.doclib.util.{DoclibFlags, FileHash, TargetPath}
import io.mdcatapult.klein.queue.Sendable
import org.apache.tika.Tika
import org.apache.tika.io.TikaInputStream
import org.apache.tika.metadata.{Metadata, TikaMetadataKeys}
import org.bson.codecs.configuration.CodecRegistry
import org.mongodb.scala.MongoCollection
import org.mongodb.scala.bson.ObjectId
import org.mongodb.scala.bson.conversions.Bson
import org.mongodb.scala.model.Filters.{and, equal, exists, or}
import org.mongodb.scala.model.Sorts._
import org.mongodb.scala.model.Updates._
import org.mongodb.scala.result.UpdateResult

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContextExecutor, Future}
import scala.util.{Failure, Success, Try}

/**
 * Handler to perform prefetch of source supplied in Prefetch Messages
 * Will automatically identify file and create a new entry in the document library
 * if it is a "remote" file it will attempt to retrieve the file and ensure consistency
 * all files receive an md5 hash of its contents if there is a detectable difference between
 * hashes it will attempt to archive and update appropriately
 *
 * @param downstream downstream queue to push Document Library messages onto
 * @param archiver queue to push all archived documents to
 * @param ac ActorSystem
 * @param materializer ActorMateriaizer
 * @param ex ExecutionContext
 * @param config Config
 * @param collection MongoCollection[Document] to read documents from
 * @param codecs CodecRegistry
 */
class PrefetchHandler(downstream: Sendable[DoclibMsg], archiver: Sendable[DoclibMsg])
                     (implicit ac: ActorSystem,
                      materializer: ActorMaterializer,
                      ex: ExecutionContextExecutor,
                      config: Config,
                      collection: MongoCollection[DoclibDoc],
                      codecs: CodecRegistry
                     ) extends LazyLogging with FileHash with TargetPath {

  /** Initialise Apache Tika && Remote Client **/
  lazy val tika = new Tika()
  lazy val remoteClient = new RemoteClient()
  lazy val flags = new DoclibFlags(config.getString("doclib.flag"))

  val doclibRoot: String = s"${config.getString("doclib.root").replaceFirst("""/+$""", "")}/"

  /**
   * Case class for handling the various permutations of local and remote documents
   *
   * @param doc      Document
   * @param origin   PrefetchOrigin
   * @param download DownloadResult
   */
  sealed case class FoundDoc(doc: DoclibDoc, archiveable: Option[List[DoclibDoc]] = None, origin: Option[Origin] = None, download: Option[DownloadResult] = None)

  /**
   * handle msg from rabbitmq
   *
   * @param msg PrefetchMsg
   * @param key String
   * @return
   */
  def handle(msg: PrefetchMsg, key: String): Future[Option[Any]] = {
    (for {
      found: FoundDoc ← OptionT(findDocument(toUri(msg.source.replaceFirst(s"^$doclibRoot", ""))))
      started: UpdateResult ← OptionT(flags.start(found.doc))
      result ← OptionT(process(found, msg))
      _ <- OptionT(processParent(msg))
      _ <- OptionT(flags.end(found.doc, started.getModifiedCount > 0))

    } yield (result, found.doc)).value.andThen({
      case Success(r) ⇒ r match {
        //TODO What is v?
        case Some(v) ⇒ logger.info(f"COMPLETED: ${msg.source} - ${v._2._id.toString}")
        //case Some(v) ⇒ println(v)
        case None ⇒ // do nothing for now, but need to identify the use case
      }
      case Failure(_) ⇒
        // enforce error flag
        Try(Await.result(findDocument(toUri(msg.source)), Duration.Inf)) match {
          case Success(value: Option[FoundDoc]) ⇒ value match {
            case Some(found) ⇒ flags.error(found.doc, noCheck = true)
            case _ ⇒ // do nothing as error handling will capture
          }
          case Failure(_) ⇒ // do nothing as error handling will capture
        }
    })
  }


  /**
   * Update parent "origin" documents with the new source for the derivative
   * @param msg PrefetchMsg
   * @return
   */
  def processParent(msg: PrefetchMsg): Future[Option[UpdateResult]] = {
    if (msg.derivative.getOrElse(false)) {
      // TODO maybe parent should be a field in the doc rather than somewhere in the origin list
      // Create list of filters by _id, one for each parent origin
      val originFilter = msg.origin.get.filter(origin => origin.scheme == "mongodb").map(parent => equal("_id", new ObjectId(parent.metadata.get.filter(m => m.getKey == "_id").head.getValue.toString)))
      val path = msg.source.replaceFirst(config.getString("doclib.local.temp-dir"), config.getString("doclib.local.target-dir"))
      // TODO get metadata from old derivative
      val derivative: Derivative = Derivative(
      `type` = "unarchived",
      path = path
      )
      collection.updateMany(or(originFilter: _*), push("derivatives", derivative)).toFutureOption().andThen({
          case Success(_) ⇒ {
            collection.updateMany(or(originFilter: _*), pull("derivatives", equal("path", msg.source))).toFutureOption().andThen({
              case Success(_) ⇒ {
              }
              // TODO does this need to bubble up?
              case Failure(e) => logger.error(s"Failed to update parent doc with new child path. $e")
            })
          }
          // TODO does this need to bubble up?
          case Failure(e) => logger.error(s"Failed to update parent doc with new child path. $e")
        })
      }
      else {
      // No derivative.
      Future.successful(None)
    }
  }

  def parentId(metadata: List[MetaValueUntyped]): Any = {
    val origin:List[MetaValueUntyped] = metadata.filter(m => m.getKey == "_id")
    origin.head.getValue
  }

  /**
   * process the found documents and generate an update to apply to the document before pushing downstream
   * @param found FoundDoc
   * @param msg PrefetchMsg
   * @return
   */
  def process(found: FoundDoc, msg: PrefetchMsg): Future[Option[Any]] = {
    val update = combine(
      getDocumentUpdate(found, msg),
      addEachToSet("tags", msg.tags.getOrElse(List[String]()).distinct:_*),
      set("metadata", msg.metadata.getOrElse(List[MetaValueUntyped]())),
      set("derivative", msg.derivative.getOrElse(false)),
      set("derivatives", found.doc.derivatives.getOrElse(List[Derivative]())),
      set("updated", LocalDateTime.now())
    )
    //processParent(found.doc.source, found.doc.origin.get) if (msg.derivative.get)
    collection.updateOne(equal("_id", found.doc._id), update).toFutureOption().andThen({
      case Success(_) ⇒ {
        downstream.send(DoclibMsg(id = found.doc._id.toString))
      }
      case Failure(e) => throw e
    })
  }

  /**
   * build consolidated list of origins from doc and msg
   * @param doc Document
   * @param msg PrefetchMsg
   * @return
   */
  def consolidateOrigins(doc: DoclibDoc, msg: PrefetchMsg): List[Origin] = {
    (doc.origin.getOrElse(List[Origin]()) ::: msg.origin.getOrElse(List[Origin]())).distinct
  }

  /**
   * moves a file on the file system from its source path to an new root location maintaining the path and prefixing the filename
   * @param source current relative source path
   * @param target relative target path to move file to
   * @return
   */
  def moveFile(source: String, target: String): Option[Path] = moveFile(
    Paths.get(s"$doclibRoot$source").toAbsolutePath.toFile,
    Paths.get(s"$doclibRoot$target").toAbsolutePath.toFile
  ) match {
    case Success(_) ⇒ Some(Paths.get(target))
    case Failure(err) ⇒ throw err
  }

  /**
   * moves a file on the file system from its source path to an new root location maintaining the path and prefixing the filename
   * @param source current absolute source path
   * @param target absolute target path to move file to
   * @return
   */
  def moveFile(source: File, target: File): Try[Path] = {
    Try({
      if (source == target) {
        target.toPath
      } else {
        target.getParentFile.mkdirs
        Files.move(source.toPath, target.toPath, StandardCopyOption.REPLACE_EXISTING)
      }
    })
  }

  /**
   * Copies a file to a new location
   * @param source source path
   * @param target target path
   * @return
   */
  def copyFile(source: String, target: String): Option[Path] = copyFile(
    Paths.get(s"$doclibRoot$source").toAbsolutePath.toFile,
    Paths.get(s"$doclibRoot$target").toAbsolutePath.toFile
  ) match {
    case Success(_) ⇒ Some(Paths.get(target))
    case Failure(_: FileNotFoundException) ⇒ None
    case Failure(err) ⇒ throw err
  }

  /**
   * Copies a file to a new location
   * @param source source path
   * @param target target path
   * @return
   */
  def copyFile(source: File, target: File): Try[Path] =
    Try({
      target.getParentFile.mkdirs
      Files.copy(source.toPath, target.toPath, StandardCopyOption.REPLACE_EXISTING)
    })

  /**
   * Silently remove file and empty parent dirs
   * @param source String
   */
  def removeFile(source:String): Unit = removeFile(Paths.get(s"$doclibRoot$source").toAbsolutePath.toFile)


  /**
   * Silently remove file and empty parent dirs
   * @param file File
   */
  def removeFile(file: File): Unit = {
    if (file.isFile || (file.isDirectory && file.listFiles.isEmpty)) {
      file.delete
      removeFile(file.getParentFile)
    }
  }

  /**
   * tests if source string starts with the configured remote target-dir
   * @param source String
   * @return
   */
  def inRemoteRoot(source: String): Boolean =
    source.startsWith(s"${config.getString("doclib.remote.target-dir")}/")

  /**
   * tests if source string starts with the configured local target-dir
   * @param source String
   * @return
   */
  def inLocalRoot(source: String): Boolean =
    source.startsWith(s"${config.getString("doclib.local.target-dir")}/")

  /**
   * Tests if found Document currently in the remote root and is not returns the appropriate download target
   * @param foundDoc Found Document and remote data
   * @return
   */
  def getRemoteUpdateTargetPath(foundDoc: FoundDoc): Option[String] = {
    if (inRemoteRoot(foundDoc.doc.source))
      Some(Paths.get(s"${foundDoc.doc.source}").toString)
    else
      Some(Paths.get(s"${foundDoc.download.get.target.get}").toString.replaceFirst(s"^$doclibRoot/*", ""))
  }

  /**
   * determines appropriate local target path if required
   * @param foundDoc Found Doc
   * @return
   */
  def getLocalUpdateTargetPath(foundDoc: FoundDoc): Option[String] = {
    if (inLocalRoot(foundDoc.doc.source))
      Some(Paths.get(s"${foundDoc.doc.source}").toString)
    else {
      // strips temp dir if present plus any prefixed slashes
      val relPath = foundDoc.doc.source.replaceFirst(s"^$doclibRoot/*", "").replaceFirst(s"^${config.getString("doclib.local.temp-dir")}/*", "")
      // ensures target dir is prepended
      val root = config.getString("doclib.local.target-dir").replaceFirst("/+$", "")
      Some(Paths.get(s"$root/$relPath").toString)
    }
  }

  /**
   *
   * @param foundDoc document to be archived
   * @param archiveSource string file to copy
   * @param archiveTarget string location to archive the document to
   * @return
   */
  def archiveDocument(foundDoc: FoundDoc, archiveSource: String, archiveTarget: String): Future[Option[Path]] =
    (for {
      archivePath: Path ← OptionT.fromOption[Future](copyFile(archiveSource, archiveTarget))
      _ ← OptionT.liftF(sendDocumentsToArchiver(foundDoc.archiveable))
    } yield archivePath).value

  /**
   * sends documents to archiver for processing.
   *
   * @todo send errors to queue without killing the rest of the process
   */
  def sendDocumentsToArchiver(docs: Option[List[DoclibDoc]]): Future[Unit] = {
    Try(docs.getOrElse(List()).foreach(doc ⇒ archiver.send(DoclibMsg(doc._id.toHexString)))) match {
      case Success(_) ⇒ Future.successful()
      case Failure(_) ⇒
        // send to error handling?
        Future.successful()
    }
  }

  //  /**
  //    * updates a physical file
  //    *  - copies existing file to archive location
  //    *  - adds document to archive collection
  //    *  - moves new file to target/document-source location
  //    * @param foundDoc FoundDoc
  //    * @param temp path that the new file is located at
  //    * @param archive the path that the file needs to be copied to
  //    * @param target an optional path to set the new source to if not using the source from the document
  //    * @return path of the target/document-source location
  //    */
  def updateFile(foundDoc: FoundDoc, temp: String, archive: String, target: Option[String] = None): Future[Option[Path]] = {
    val targetSource = target.getOrElse(foundDoc.doc.source)
    archiveDocument(foundDoc, targetSource, archive).map(_ ⇒ moveFile(temp, targetSource))
  }

  /**
   * generate an archive for the found document
   * @param targetPath the found doc
   * @return
   */
  def getArchivePath(targetPath: String, hash: String): String = {
    val  withExt = """(.*)/(.*)\.(.+)$""".r
    val withoutExt = """(.*)/(.*)$""".r
    targetPath match {
      case withExt(path, file, ext) ⇒ s"${getTargetPath(path, config.getString("doclib.archive.target-dir"))}$file.$ext/$hash.$ext"
      case withoutExt(path, file) ⇒ s"${getTargetPath(path, config.getString("doclib.archive.target-dir"))}$file/$hash"
      case _ ⇒ throw new Exception("Unable to identify path and filename")
    }
  }

  /**
   * Handles the potential update of a document and is associated file based on supplied properties
   * @param foundDoc the found document
   * @param tempPath the path of the temporary file either remote or local
   * @param targetPathGenerator function to generate the absolute target path for the file
   * @param inRightLocation function to test if the current document source path is in the right location
   * @return
   */
  def handleFileUpdate(foundDoc: FoundDoc, tempPath: String, targetPathGenerator: FoundDoc ⇒ Option[String], inRightLocation: String ⇒ Boolean): Option[Path] = {
    val docHash: String = foundDoc.doc.hash
    targetPathGenerator(foundDoc) match {
      case Some(targetPath) ⇒
        val absTargetPath = Paths.get(s"$doclibRoot$targetPath").toAbsolutePath
        val currentHash = if (absTargetPath.toFile.exists()) md5(absTargetPath.toString) else docHash
        if (docHash != currentHash)
        // file already exists at target location but is not the same file, archive the old one then add the new one
          Await.result(updateFile(foundDoc, tempPath, getArchivePath(targetPath, currentHash), Some(targetPath)), Duration.Inf)
        else if (!inRightLocation(foundDoc.doc.source))
          moveFile(tempPath, targetPath)
        else { // not a new file or a file that requires updating so we will just cleanup the temp file
          removeFile(tempPath)
          None
        }
      case None ⇒ None
    }
  }

  /**
   * Builds a document update with updates source and origins
   *
   * @param foundDoc FoundDoc
   * @param msg    PrefetchMsg
   * @return Bson
   */
  def getDocumentUpdate(foundDoc: FoundDoc, msg: PrefetchMsg): Bson = {
    val currentOrigins: List[Origin] = consolidateOrigins(foundDoc.doc, msg)
    val (source: Option[Path], origin: List[Origin]) = foundDoc.download match {
      case Some(downloaded) ⇒
        val source = handleFileUpdate(foundDoc, downloaded.source, getRemoteUpdateTargetPath, inRemoteRoot)
        val filteredDocOrigin = currentOrigins.filterNot(d ⇒ d.uri.toString == foundDoc.origin.get.uri.toString)
        (source, foundDoc.origin.get :: filteredDocOrigin)

      case None ⇒
        val source = handleFileUpdate(foundDoc, msg.source, getLocalUpdateTargetPath, inLocalRoot)
        (source ,currentOrigins)
    }
    // source needs to be relative path from doclib.root
    combine(
      set("source", source match {
        case Some(path: Path) ⇒ path.toString.replaceFirst(s"^${config.getString("doclib.root")}", "")
        case None ⇒ foundDoc.doc.source.replaceFirst(s"^${config.getString("doclib.root")}", "")
      }),
      set("origin", origin),
      getFileAttrs(source),
      getMimetype(source)
    )
  }

  /**
   * builds a mongo update based on the target files attributes
   *
   * @param source Path relative path from doclib.root
   * @return Bson $set
   */
  def getFileAttrs(source: Option[Path]): Bson = {
    source match {
      case Some(path) ⇒
        val absPath = (doclibRoot/path.toString).path
        val attrs = Files.getFileAttributeView(absPath, classOf[BasicFileAttributeView]).readAttributes()
        set(config.getString("prefetch.labels.attrs"), FileAttrs(
          path = absPath.getParent.toAbsolutePath.toString,
          name = absPath.getFileName.toString,
          mtime = LocalDateTime.ofInstant(attrs.lastModifiedTime().toInstant, ZoneOffset.UTC),
          ctime = LocalDateTime.ofInstant(attrs.creationTime().toInstant, ZoneOffset.UTC),
          atime = LocalDateTime.ofInstant(attrs.lastAccessTime().toInstant, ZoneOffset.UTC),
          size = attrs.size()
        ))
      case None ⇒ combine()
    }
  }

  /**
   * detect mimetype of source file
   *
   * @param source Path relative path from doclib.root
   * @return Bson $set
   */
  def getMimetype(source: Option[Path]): Bson =
    source match {
      case Some(path) ⇒
        val absPath = (doclibRoot/path.toString).path
        val metadata = new Metadata()
        metadata.set(TikaMetadataKeys.RESOURCE_NAME_KEY, absPath.getFileName.toString)
        set(config.getString("prefetch.labels.mimetype"), tika.getDetector.detect(
          TikaInputStream.get(new FileInputStream(absPath.toString)),
          metadata
        ).toString)
      case None ⇒ combine()
    }

  /**
   * retrieves a document based on url or filepath
   *
   * @param uri io.lemonlabs.uri.Uri
   * @return
   */
  def findDocument(uri: Uri): Future[Option[FoundDoc]] =
    uri.schemeOption match {
      case None ⇒ throw new UndefinedSchemeException(uri)
      case Some("file") ⇒ findLocalDocument(uri)
      case _ ⇒ findRemoteDocument(uri)
    }


  /**
   * retrieves document from mongo based on supplied uri being for a local source
   *
   * @param uri io.lemonlabs.uri.Uri
   * @return
   */
  def findLocalDocument(uri: Uri): Future[Option[FoundDoc]] =
    (for {
      target: String ← OptionT.some[Future](uri.path.toString.replaceFirst(
        s"^${config.getString("doclib.local.temp-dir")}",
        config.getString("doclib.local.target-dir")
      ))
      md5 ← OptionT.some[Future](md5(s"$doclibRoot${uri.path.toString}"))
      (doc, archivable) ← OptionT(findOrCreateDoc(uri.path.toString, md5, Some(or(
        equal("source", uri.path.toString),
        equal("source", target)
      ))))
    } yield FoundDoc(doc, archivable)).value


  /**
   * retrieves document from mongo based on supplied uri being for a remote source
   *
   * @param uri io.lemonlabs.uri.Uri
   * @return
   */
  def findRemoteDocument(uri: Uri): Future[Option[FoundDoc]] =
    (for { // assumes remote
      origin: Origin ← OptionT.liftF(remoteClient.resolve(uri))
      downloaded: DownloadResult ← OptionT.fromOption[Future](remoteClient.download(origin.uri.get))
      (doc, archivable) ← OptionT(findOrCreateDoc(origin.uri.get.toString, downloaded.hash, Some(or(
        equal("origin.uri", origin.uri.get.toString),
        equal("source", origin.uri.get.toString)
      ))))
           } yield FoundDoc(doc, archivable, origin = Some(origin), download = Some(downloaded))).value


  /**
   * retrieves document from mongo if no document found will create and persist new document
   *
   * @param source String
   * @param hash String
   * @return
   */
  def findOrCreateDoc(source: String, hash: String, query: Option[Bson] = None): Future[Option[(DoclibDoc, Option[List[DoclibDoc]])]] = {
    collection.find(
      or(
        equal("hash", hash),
        query.getOrElse(combine())
      )
    ).sort(descending("created")).toFuture()
      .map({
        case latest :: archivable ⇒
          if (latest.hash == hash) {
            Some(latest, Some(archivable))
          } else {
            Some(createDoc(source, hash), Some(latest :: archivable))
          }
        case Nil ⇒ Some(createDoc(source, hash), None)
      })
  }

  def createDoc(source: String, hash: String): DoclibDoc = {
    val createdTime = LocalDateTime.now().toInstant(ZoneOffset.UTC)
    //val path = ScalaFile(source).path
    //val attrs = Files.getFileAttributeView(path, classOf[BasicFileAttributeView]).readAttributes()
    //TODO What should created, modified, accessed time be
    //    val fileAttrs = FileAttrs(
    //      path = path.getParent.toAbsolutePath.toString,
    //      name = path.getFileName.toString,
    //      mtime = LocalDateTime.ofInstant(createdTime, ZoneOffset.UTC),
    //      ctime = LocalDateTime.ofInstant(createdTime, ZoneOffset.UTC),
    //      atime = LocalDateTime.ofInstant(createdTime, ZoneOffset.UTC),
    //      size = attrs.size()
    //    )
    //TODO what should created and updated time be. Mime type? From getMimeType or from some metadata? More than one?
    // At this point the file has not been copied to its final destination so no FileAttrs
    val newDoc = DoclibDoc(
      _id = new ObjectId(),
      source = source,
      hash = hash,
      derivative = false,
      created = LocalDateTime.ofInstant(createdTime, ZoneOffset.UTC),
      updated = LocalDateTime.ofInstant(createdTime, ZoneOffset.UTC),
      mimetype = "",
      tags = Some(List[String]())
    )
    Await.result(collection.insertOne(newDoc).toFutureOption(), Duration.Inf)
    newDoc
  }

  /**
   * converts provided source into valid Uri object
   *
   * @param source String
   * @return
   */
  def toUri(source: String): Uri = {
    Uri.parseTry(source) match {
      case Success(uri) ⇒ uri.schemeOption match {
        case Some(_) ⇒ uri
        case None ⇒ uri.withScheme("file")
      }
      case Failure(_) ⇒ throw new Exception(s"unable to convert '$source' into valid Uri Object")
    }
  }

}
