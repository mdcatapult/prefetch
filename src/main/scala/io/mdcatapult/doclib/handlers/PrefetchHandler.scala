package io.mdcatapult.doclib.handlers

import java.io.{File, FileInputStream, FileNotFoundException}
import java.nio.file.attribute.BasicFileAttributeView
import java.nio.file.{Files, Path, Paths, StandardCopyOption}
import java.time.{LocalDateTime, ZoneOffset}

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import cats.data._
import cats.implicits._
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import io.lemonlabs.uri.Uri
import io.mdcatapult.doclib.messages.{DoclibMsg, PrefetchMsg}
import io.mdcatapult.doclib.models.metadata._
import io.mdcatapult.doclib.models.{FileAttrs, PrefetchOrigin}
import io.mdcatapult.doclib.remote.{DownloadResult, UndefinedSchemeException, Client ⇒ RemoteClient}
import io.mdcatapult.doclib.util.{DoclibFlags, FileHash, TargetPath}
import io.mdcatapult.klein.queue.Queue
import org.apache.tika.Tika
import org.apache.tika.io.TikaInputStream
import org.apache.tika.metadata.{Metadata, TikaMetadataKeys}
import org.bson.codecs.configuration.CodecRegistry
import org.mongodb.scala.bson.conversions.Bson
import org.mongodb.scala.bson.{BsonArray, BsonDateTime, BsonString, BsonValue, ObjectId}
import org.mongodb.scala.model.Filters.{equal, or}
import org.mongodb.scala.model.Updates.{addEachToSet, combine, set}
import org.mongodb.scala.result.UpdateResult
import org.mongodb.scala.{Completed, Document, MongoCollection}
import play.api.libs.json.{JsSuccess, Json}

import scala.collection.JavaConverters._
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
  * @param downstream downstream queue to push Document LIbrary messages onto
  * @param archiveCollection collection to push all archived documents to
  * @param ac ActorSystem
  * @param materializer ActorMateriaizer
  * @param ex ExecutionContext
  * @param config Config
  * @param collection MongoCollection[Document] to read documents from
  * @param codecs CodecRegistry
  */
class PrefetchHandler(downstream: Queue[DoclibMsg], archiveCollection: MongoCollection[Document])
                     (implicit ac: ActorSystem,
                      materializer: ActorMaterializer,
                      ex: ExecutionContextExecutor,
                      config: Config,
                      collection: MongoCollection[Document],
                      codecs: CodecRegistry
                     ) extends LazyLogging with FileHash with TargetPath {

  /** Initialise Apache Tika && Remote Client **/
  lazy val tika = new Tika()
  lazy val remoteClient = new RemoteClient()
  lazy val flags = new DoclibFlags(config.getString("doclib.flag"))

  /**
    * Case class for handling the various permutations of local and remote documents
    * @param doc Document
    * @param origin PrefetchOrigin
    * @param download DownloadResult
    */
  sealed case class FoundDoc(doc: Document, origin: Option[PrefetchOrigin] = None, download: Option[DownloadResult] = None)


  /**
    * handle msg from rabbitmq
    *
    * @param msg PrefetchMsg
    * @param key String
    * @return
    */
  def handle(msg: PrefetchMsg, key: String): Future[Option[Any]] = (for {
    found: FoundDoc ← OptionT(fetchDocument(toUri(msg.source)))
    started: UpdateResult ← OptionT(flags.start(found.doc))
    result ← OptionT(process(found, msg))
    _ <- OptionT(flags.end(found.doc, started.getModifiedCount > 0))
  } yield (result, found.doc)).value.andThen({
    case Success(r) ⇒ r match {
      case Some(v) ⇒ logger.info(f"COMPLETED: ${msg.source} - ${v._2.getObjectId("_id").toString}")
      case None ⇒ // do nothing for now, but need to identify the use case
    }
    case Failure(err) ⇒
      // enforce error flag
      Await.result(fetchDocument(toUri(msg.source)), Duration.Inf) match {
        case Some(found) ⇒ flags.error(found.doc, noCheck = true)
        case _ ⇒ // do nothing
      }
      throw err
  })

  /**
    * process the found documents and generate an update to apply to the document before pushing downstream
    * @param found FoundDoc
    * @param msg PrefetchMsg
    * @return
    */
  def process(found: FoundDoc, msg: PrefetchMsg): Future[Option[Any]] = {
    val update = combine(
      fetchDocumentUpdate(found, msg),
      addEachToSet("tags", msg.tags.getOrElse(List[String]()).distinct:_*),
      set("metadata", fetchMetaData(msg)),
      set("derivative", msg.derivative.getOrElse(false)),
      set("updated", LocalDateTime.now())
    )

    collection.updateOne(equal("_id", found.doc.getObjectId("_id")), update).toFutureOption().andThen({
      case Success(_) ⇒ downstream.send(DoclibMsg(id = found.doc.getObjectId("_id").toString))
      case Failure(e) => throw e
    })
  }

  /**
    * build consolidated list of origins from doc and msg
    * @param doc Document
    * @param msg PrefetchMsg
    * @return
    */
  def consolidateOrigins(doc: Document, msg: PrefetchMsg): List[PrefetchOrigin] =
    (doc.get[BsonArray]("origin").getOrElse(BsonArray()).getValues.asScala.flatMap({
      case d: BsonValue ⇒ Json.parse(d.asDocument().toJson).validate[PrefetchOrigin] match {
        case JsSuccess(value, _) ⇒ Some(value)
        case _ ⇒ None
      }
      case _ ⇒ None
    }).toList ::: msg.origin.getOrElse(List[PrefetchOrigin]())).distinct



  def moveFile(source: String, target: String): Option[Path] = moveFile(new File(source), new File(target))

  /**
    * moves a file on the file system from its source path to an new root location maintaining the path and prefixing the filename
    * @param source current source path
    * @param target target path to move file to
    * @return
    */
  def moveFile(source: File, target: File): Option[Path] = {
    Try({
      if (source == target) {
        target.toPath
      } else {
        target.getParentFile.mkdirs
        Files.move(source.toPath, target.toPath, StandardCopyOption.REPLACE_EXISTING)
      }
    }) match {
      case Success(path: Path) ⇒ Some(path)
      case Failure(err) ⇒ throw err
    }
  }

  def copyFile(source: String, target: String): Option[Path] = copyFile(new File(source), new File(target))

  def copyFile(source: File, target: File): Option[Path] = {
    if (source.exists)
      Try({
        target.getParentFile.mkdirs
        Files.copy(source.toPath, target.toPath, StandardCopyOption.REPLACE_EXISTING)
      }) match {
        case Success(path: Path) ⇒ Some(path)
        case Failure(_: FileNotFoundException) ⇒ None
        case Failure(err) ⇒ throw err
      }
    else None
  }

  /**
    * Silently remove file and empty parent dirs
    * @param source String
    */
  def removeFile(source:String): Unit = removeFile(new File(source))


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
    * archives a document to the archive collection and moves file into archive folder
    * @param doc Source Document
    * @return
    */
  def addToArchiveCollection(doc: Document, archivePath: Path): Future[Option[Completed]] = {
    archiveCollection.insertOne(Document(
      "_id" → new ObjectId(),
      "created" → BsonDateTime(LocalDateTime.now().toInstant(ZoneOffset.UTC).toEpochMilli),
      "archive" → BsonString(archivePath.toAbsolutePath.toString),
      "document" → doc
    )).toFutureOption()
  }

  /**
    * tests if source string starts with the configured remote target-dir
    * @param source String
    * @return
    */
  def inRemoteRoot(source: String): Boolean =
    source.startsWith(new File(config.getString("prefetch.remote.target-dir")).getAbsolutePath)

  /**
  * tests if source string starts with the configured local target-dir
  * @param source String
  * @return
  */
  def inLocalRoot(source: String): Boolean =
    source.startsWith(new File(config.getString("prefetch.local.target-dir")).getAbsolutePath)

  /**
    * Tests if found Document currently in the remote root and is not returns the appropriate download target
    * @param foundDoc Found Document and remote data
    * @return
    */
  def getRemoteUpdateTargetPath(foundDoc: FoundDoc): Option[String] =
    if (inRemoteRoot(foundDoc.doc.getString("source")))
      Some(foundDoc.doc.getString("source"))
    else
      foundDoc.download.get.target

  /**
    * determines appropriate local target path if required
    * @param foundDoc Found Doc
    * @return
    */
  def getLocalUpdateTargetPath(foundDoc: FoundDoc): Option[String] =
    if (inLocalRoot(foundDoc.doc.getString("source")))
      Some(foundDoc.doc.getString("source"))
    else {
      // strips temp dir if present plus any prefixed slashes
      val relPath = foundDoc.doc.getString("source").replaceAll(config.getString("prefetch.local.temp-dir"), "").replaceAll("^/+", "")
      // ensures target dir is prepended
      val root = config.getString("prefetch.local.target-dir").replaceAll("/+$", "")
      Some(Paths.get(s"$root/$relPath").toAbsolutePath.toString)
    }

  /**
    *
    * @param foundDoc document to be archived
    * @param archive string location to store the document
    * @return
    */
  def archiveDocument(foundDoc: FoundDoc, archive: String): Future[Option[Completed]] =
    (for {
      archivePath: Path ← OptionT.fromOption[Future](copyFile(foundDoc.doc.getString("source"), archive))
      result: Completed ← OptionT(addToArchiveCollection(foundDoc.doc, archivePath))
    } yield result).value

  /**
    * updates a physical file
    *  - copies existing file to archive location
    *  - adds document to archive collection
    *  - moves new file to target/document-source location
    * @param foundDoc FoundDoc
    * @param temp path that the new file is located at
    * @param archive the path that the file needs to be copied to
    * @param target an optional path to set the new source to if not using the source from the document
    * @return path of the target/document-source location
    */
  def updateFile(foundDoc: FoundDoc, temp: String, archive: String, target: Option[String] = None): Future[Option[Path]] =
    archiveDocument(foundDoc, archive).map(_ ⇒ moveFile(temp, target.getOrElse(foundDoc.doc.getString("source"))))

  /**
    * generate an archive for the found document
    * @param foundDoc the found doc
    * @return
    */
  def getArchivePath(foundDoc: FoundDoc): String = {
    val currentPath = Paths.get(foundDoc.doc.getString("source")).toAbsolutePath
    val archiveDir =
      getTargetPath(
        currentPath.toString,
        new File(config.getString("prefetch.archive.target-dir")).getAbsolutePath
      )
    s"$archiveDir${foundDoc.doc.getString("hash")}_${currentPath.getFileName.toString}"
  }

  def handleFileUpdate(foundDoc: FoundDoc, newHash: String, source: String, targetPathGenerator: FoundDoc ⇒ Option[String], inRightLocation: String ⇒ Boolean): Option[Path] = {
    val currentHash: Option[BsonString] = foundDoc.doc.get[BsonString]("hash")
    targetPathGenerator(foundDoc) match {
      case Some(targetPath) ⇒
        if (currentHash.nonEmpty && currentHash.get.getValue != newHash)
          Await.result(updateFile(foundDoc, source, getArchivePath(foundDoc), Some(targetPath)), Duration.Inf)
        else if (currentHash.nonEmpty && !inRightLocation(foundDoc.doc.getString("source")))
          moveFile(foundDoc.doc.getString("source"), targetPath)
        else if (currentHash.isEmpty)
          moveFile(source, targetPath)
        else { // not a new file or a file that requires updating so we will just cleanup the temp file
          removeFile(source)
          None
        }
      case None ⇒ None
    }
  }

  /**
    * checks to see if a remote file was downloaded as part found document
    * if a download is present and the hash is different from the current file it will
    *  - archive the old file,
    *  - move the new file into its new location
    *  - and create a suitable update that defines the new source path, updates the origins and adds the new file
    * if a download is present and the hash has not changed it will
    *  - verify if the current source path is in the remote root and it not move the file with an appropriate update
    * if no download is present then it will just update the origins
    *
    * @todo super heavy function in need of revision/refactor
    * @param foundDoc FoundDoc
    * @param msg    PrefetchMsg
    * @return ("Bson $set for origin, source", "identified source string")
    */
  def fetchDocumentUpdate(foundDoc: FoundDoc, msg: PrefetchMsg): Bson = {
    val currentOrigins: List[PrefetchOrigin] = consolidateOrigins(foundDoc.doc, msg)
    val (source: Option[Path], origin: List[PrefetchOrigin]) = foundDoc.download match {
      case Some(downloaded) ⇒
        val source: Option[Path] = handleFileUpdate(
          foundDoc,
          downloaded.hash,
          downloaded.source,
          getRemoteUpdateTargetPath,
          inRemoteRoot)
        val filteredDocOrigin = currentOrigins.filterNot(d ⇒ d.uri.toString == foundDoc.origin.get.uri.toString)
        (source, foundDoc.origin.get :: filteredDocOrigin)

      case None ⇒
        val source: Option[Path] = handleFileUpdate(
          foundDoc,
          md5(msg.source),
          msg.source,
          getLocalUpdateTargetPath,
          inLocalRoot)
        (source ,currentOrigins)
    }

    combine(
      set("source", source match {
        case Some(path: Path) ⇒ path.toAbsolutePath.toString
        case None ⇒ foundDoc.doc.getString("source")
      }),
      set("origin", origin),
      fetchFileAttrs(source),
      fetchMimetype(source),
      fetchFileHash(source),
    )
  }

  /**
    * builds a mongo update based on the target files attributes
    *
    * @param source String
    * @return Bson $set
    */
  def fetchFileAttrs(source: Option[Path]): Bson =
    source match {
      case Some(path) ⇒
        val attrs = Files.getFileAttributeView(path, classOf[BasicFileAttributeView]).readAttributes()
        set(config.getString("prefetch.labels.attrs"), FileAttrs(
          path = path.getParent.toAbsolutePath.toString,
          name = path.getFileName.toString,
          mtime = LocalDateTime.ofInstant(attrs.lastModifiedTime().toInstant, ZoneOffset.UTC),
          ctime = LocalDateTime.ofInstant(attrs.creationTime().toInstant, ZoneOffset.UTC),
          atime = LocalDateTime.ofInstant(attrs.lastAccessTime().toInstant, ZoneOffset.UTC),
          size = attrs.size()
        ))
      case None ⇒ combine()
    }

  /**
    * detect mimetype of source file
    *
    * @param source String
    * @return Bson $set
    */
  def fetchMimetype(source: Option[Path]): Bson =
    source match {
      case Some(path) ⇒
        val metadata = new Metadata()
        metadata.set(TikaMetadataKeys.RESOURCE_NAME_KEY, path.getFileName.toString)
        set(config.getString("prefetch.labels.mimetype"), tika.getDetector.detect(
          TikaInputStream.get(new FileInputStream(path.toString)),
          metadata
        ).toString)
      case None ⇒ combine()
  }

  /**
    * generate md5 hash of file
    *
    * @param source String
    * @return Bson $set
    */
  def fetchFileHash(source: Option[Path]): Bson =
    source match {
      case Some(path) ⇒ set(config.getString("prefetch.labels.hash"), md5(path.toAbsolutePath.toString))
      case None ⇒ combine()
    }




  /**
    * retrieves a document based on url or filepath
    *
    * @param uri io.lemonlabs.uri.Uri
    * @return
    */
  def fetchDocument(uri: Uri): Future[Option[FoundDoc]] =
    uri.schemeOption match {
      case None ⇒ throw new UndefinedSchemeException(uri)
      case Some("file") ⇒ (for {
        target: String ← OptionT.some[Future](uri.path.toString.replaceFirst(
          config.getString("prefetch.local.temp-dir"),
          config.getString("prefetch.local.target-dir"))
        )
        doc: Document ← OptionT(findOrCreateDoc(
          or(
            // try finding a matching document using hot folder path or the expected target path
            or(
              equal("source", uri.path.toString),
              equal("source", target)
            ),
            equal("hash", md5(uri.path.toString))
          ), uri.path.toString))
      } yield FoundDoc(doc = doc)).value
      case _ ⇒ (for { // assumes remote
        origin: PrefetchOrigin ← OptionT.liftF(remoteClient.resolve(uri))
        downloaded: DownloadResult ← OptionT.fromOption[Future](remoteClient.download(origin.uri.get))
        doc: Document ← OptionT(findOrCreateDoc(
          or(
            equal("origin.uri", origin.uri.get.toString),
            equal("source", origin.uri.get.toString),
            equal("hash", downloaded.hash)
          ),
          origin.uri.get.toString)
        )
      } yield FoundDoc(doc = doc, origin = Some(origin), download = Some(downloaded))).value
    }

  /**
    * Retrieves metadata from the PrefetchMsg and converts into Bson compatible objects
    * @param msg PrefetchMsg
    * @return
    */
    def fetchMetaData(msg: PrefetchMsg): List[MetaValue] = {
      msg.metadata.getOrElse(Map[String, Any]()).map(m ⇒ m._2 match {
        case v: Int ⇒ MetaInt(m._1, v)
        case v: Double ⇒ MetaDouble(m._1, v)
        case v: String ⇒ MetaString(m._1, v)
        case _ ⇒ throw new Exception("Metadata value type not currently implemented")
      }).toList
    }

  /**
    * retrieves document from mongo if no document found will create and persist new document
    *
    * @param query  Bson
    * @param source String
    * @return
    */
  def findOrCreateDoc(query: Bson, source: String): Future[Option[Document]] =
    collection.find(query).first().toFutureOption().map({
      case Some(found) ⇒ Some(found)
      case None ⇒
        val newDoc = Document(
          "_id" → new ObjectId(),
          "source" → source,
          "created" → BsonDateTime(LocalDateTime.now().toInstant(ZoneOffset.UTC).toEpochMilli),
          "doclib" → BsonArray())
        Await.result(collection.insertOne(newDoc).toFutureOption(), Duration.Inf)
        Some(newDoc)
    })

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
