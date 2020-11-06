package io.mdcatapult.doclib.handlers

import java.io.{File, FileInputStream, FileNotFoundException}
import java.nio.file.attribute.BasicFileAttributeView
import java.nio.file.{Files, Path, Paths, StandardCopyOption}
import java.time.{LocalDateTime, ZoneOffset}
import java.util.UUID

import akka.stream.Materializer
import better.files._
import cats.data._
import cats.implicits._
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import io.lemonlabs.uri.Uri
import io.mdcatapult.doclib.exception.DoclibDocException
import io.mdcatapult.doclib.flag.{FlagContext, MongoFlagStore}
import io.mdcatapult.doclib.messages.{DoclibMsg, PrefetchMsg}
import io.mdcatapult.doclib.metrics.Metrics._
import io.mdcatapult.doclib.models._
import io.mdcatapult.doclib.models.metadata._
import io.mdcatapult.doclib.path.TargetPath
import io.mdcatapult.doclib.remote.adapters.{Ftp, Http}
import io.mdcatapult.doclib.remote.{DownloadResult, UndefinedSchemeException, Client => RemoteClient}
import io.mdcatapult.doclib.util.Metrics.fileOperationLatency
import io.mdcatapult.klein.queue.Sendable
import io.mdcatapult.util.concurrency.LimitedExecution
import io.mdcatapult.util.hash.Md5.md5
import io.mdcatapult.util.models.Version
import io.mdcatapult.util.models.result.UpdatedResult
import io.mdcatapult.util.time.nowUtc
import org.apache.tika.Tika
import org.apache.tika.io.TikaInputStream
import org.apache.tika.metadata.{Metadata, TikaMetadataKeys}
import org.mongodb.scala.MongoCollection
import org.mongodb.scala.bson.ObjectId
import org.mongodb.scala.bson.conversions.Bson
import org.mongodb.scala.model.Filters.{equal, or}
import org.mongodb.scala.model.Sorts._
import org.mongodb.scala.model.Updates._
import org.mongodb.scala.result.{InsertOneResult, UpdateResult}

import scala.annotation.tailrec
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

/**
 * Handler to perform prefetch of source supplied in Prefetch Messages
 * Will automatically identify file and create a new entry in the document library
 * if it is a "remote" file it will attempt to retrieve the file and ensure consistency
 * all files receive an md5 hash of its contents if there is a detectable difference between
 * hashes it will attempt to archive and update appropriately
 *
 * @param downstream   downstream queue to push Document Library messages onto
 * @param archiver     queue to push all archived documents to
 * @param readLimiter  used to limit number of concurrent reads from Mongo
 * @param writeLimiter used to limit number of concurrent writes to Mongo
 * @param ec           ExecutionContext
 * @param m            Materializer
 * @param config       Config
 * @param collection   MongoCollection[Document] to read documents from
 */
class PrefetchHandler(downstream: Sendable[DoclibMsg],
                      archiver: Sendable[DoclibMsg],
                      readLimiter: LimitedExecution,
                      writeLimiter: LimitedExecution)
                     (implicit ec: ExecutionContext,
                      m: Materializer,
                      config: Config,
                      collection: MongoCollection[DoclibDoc],
                      derivativesCollection: MongoCollection[ParentChildMapping]
                     ) extends LazyLogging with TargetPath {

  val consumerName: String = config.getString("appName")
  /** set props for target path generation */
  override val doclibConfig: Config = config

  private val docExtractor = DoclibDocExtractor()

  /** Initialise Apache Tika && Remote Client **/
  private val tika = new Tika()
  val remoteClient = new RemoteClient()
  private val version = Version.fromConfig(config)

  private val flags = new MongoFlagStore(version, DoclibDocExtractor(), collection, nowUtc)
  private val doclibRoot: String = s"${config.getString("doclib.root").replaceFirst("""/+$""", "")}/"

  private val archiveDirName = config.getString("doclib.archive.target-dir")
  private val localDirName = config.getString("doclib.local.target-dir")
  private val remoteDirName = config.getString("doclib.remote.target-dir")

  sealed case class PrefetchUri(raw: String, uri: Option[Uri])

  /**
   * Case class for handling the various permutations of local and remote documents
   *
   * @param doc      Document
   * @param origins  PrefetchOrigin
   * @param download DownloadResult
   */
  sealed case class FoundDoc(doc: DoclibDoc, archiveable: List[DoclibDoc] = Nil, origins: List[Origin] = Nil, download: Option[DownloadResult] = None)

  /**
   * If a file has zero length then prefetch will not process it during ingest.
   *
   * @param filePath The path to the zero length file
   * @param doc      The mongodb record which references this file
   */
  class ZeroLengthFileException(filePath: String, doc: DoclibDoc) extends Exception(s"$filePath has zero length and will not be processed further. See doclib record ${doc._id} for more details.")

  /**
   * If a file has zero length then prefetch will not process it during ingest.
   *
   * @param doc The mongodb record which references this file
   */
  class SilentValidationException(doc: DoclibDoc) extends DoclibDocException(doc, "Suppressed exception for Validation")

  class InvalidOriginSchemeException(msg: PrefetchMsg) extends Exception(s"$msg contains invalid origin scheme")

  class MissingOriginSchemeException(msg: PrefetchMsg, origin: Origin) extends Exception(s"$origin has no uri: msg=$msg")

  /**
   * handle msg from rabbitmq
   *
   * @param msg PrefetchMsg
   * @param key String
   * @return
   */
  def handle(msg: PrefetchMsg, key: String): Future[Option[Any]] = {
    logger.info(f"RECEIVED: ${msg.source}")

    val flagContext: FlagContext = flags.findFlagContext(Some(config.getString("upstream.queue")))

    try {
      (for {
        found: FoundDoc <- OptionT(findDocument(toUri(msg.source.replaceFirst(s"^$doclibRoot", ""))))
        if !docExtractor.isRunRecently(found.doc) && valid(msg, found)
        started: UpdatedResult <- OptionT.liftF(flagContext.start(found.doc))
        newDoc <- OptionT(process(found, msg))
        _ <- OptionT.liftF(processParent(newDoc, msg))
        _ <- OptionT.liftF(flagContext.end(found.doc, noCheck = started.modifiedCount > 0))
      } yield Left((newDoc, found.doc))).value
        .recoverWith({
          case e: SilentValidationException => Future.successful(Some(Right(e)))
        })
        .andThen({
          case Success(r: Option[Either[(DoclibDoc, DoclibDoc), SilentValidationException]]) =>
            r match {
              case Some(Left(v)) =>
                handlerCount.labels(consumerName, config.getString("upstream.queue"), "success").inc()
                logger.info(f"COMPLETED: ${msg.source} - ${v._2._id}")
              case Some(Right(e)) =>
                handlerCount.labels(consumerName, config.getString("upstream.queue"), "dropped").inc()
                logger.info(f"DROPPED: ${msg.source} - ${e.getDoc._id}")
              case None =>
                throw new RuntimeException("Unknown Error Occurred")
            }
          case Failure(e: DoclibDocException) =>
            handlerCount.labels(consumerName, config.getString("upstream.queue"), "doclib_doc_exception").inc()
            logger.error("failure", e)
            flagContext.error(e.getDoc, noCheck = true)
          case Failure(e) =>
            handlerCount.labels(consumerName, config.getString("upstream.queue"), "unknown_error").inc()
            logger.error("failure", e)
            // enforce error flag
            Try(Await.result(findDocument(toUri(msg.source)), Duration.Inf)) match {
              case Success(value: Option[FoundDoc]) => value match {
                case Some(found) => flagContext.error(found.doc, noCheck = true)
                case _ => () // do nothing as error handling will capture
              }
              // There is no mongo doc - error happened before one was created
              case Failure(_) => () // do nothing as error handling will capture
            }
        })
    } catch {
      case e: Throwable =>
        logger.error("tragic error", e)
        throw e
    }
  }

  def zeroLength(filePath: String): Boolean = {
    val absPath = (doclibRoot / filePath).path
    val attrs = Files.getFileAttributeView(absPath, classOf[BasicFileAttributeView]).readAttributes()
    attrs.size == 0
  }

  /**
   * Update parent documents with the new source for the derivative.
   *
   * @param msg PrefetchMsg
   * @return
   */
  def processParent(doc: DoclibDoc, msg: PrefetchMsg): Future[Any] = {
    if (doc.derivative) {
      val path = getTargetPath(msg.source, localDirName)
      updateParentChildMappings(msg.source, path, doc._id)
    } else {
      // No derivative. Just return a success - we don't do anything with the response
      Future.successful(None)
    }
  }

  /**
   * Update any existing parent child mappings
   */
  def updateParentChildMappings(source: String, path: String, id: ObjectId): Future[UpdateResult] = {
    val latency = mongoLatency.labels(consumerName, "update_parent_child_mappings").startTimer()
    derivativesCollection.updateMany(
      equal("childPath", source),
      combine(
        set("childPath", path),
        set("child", id)
      )
    ).toFuture().andThen(_ => latency.observeDuration())
  }

  /**
   * Given existing parent and child details create parent-child mappings
   */
  def createParentChildDerivative(parentDoc: DoclibDoc, childDoc: DoclibDoc, target: String): Future[List[Option[InsertOneResult]]] = {

    val derivatives: List[Derivative] = parentDoc.derivatives.getOrElse(List())

    val mappings: List[Future[Option[InsertOneResult]]] =
      for {
        derivative <- derivatives
        latency = mongoLatency.labels(consumerName, "insert_parent_child_mapping").startTimer()
        parentChild = derivativesCollection.insertOne(
          ParentChildMapping(
            _id = UUID.randomUUID,
            parent = parentDoc._id,
            child = Some(childDoc._id),
            childPath = target,
            metadata = derivative.metadata
          )
        ).toFutureOption().andThen(_ => latency.observeDuration())
      } yield parentChild

    Future.sequence(mappings)
  }

  def parentId(metadata: List[MetaValueUntyped]): Any = {
    val origin: List[MetaValueUntyped] = metadata.filter(m => m.getKey == "_id")
    origin.head.getValue
  }

  /**
   * process the found documents and generate an update to apply to the document before pushing downstream
   *
   * @param found FoundDoc
   * @param msg   PrefetchMsg
   * @return
   */
  def process(found: FoundDoc, msg: PrefetchMsg): Future[Option[DoclibDoc]] = {
    // Note: derivatives has to be added since unarchive (and maybe others) expect this to exist in the record
    //TODO: tags and metadata are optional in a doc. addEachToSet fails if they are null. Tags is set to an empty list
    // during the prefetch process. Changed it to 'set' just in case...
    val latency = mongoLatency.labels(consumerName, "update_document").startTimer()
    val update = combine(
      getDocumentUpdate(found, msg),
      set("tags", (msg.tags.getOrElse(List[String]()) ::: found.doc.tags.getOrElse(List[String]())).distinct),
      set("metadata", (msg.metadata.getOrElse(List[MetaValueUntyped]()) ::: found.doc.metadata.getOrElse(List[MetaValueUntyped]())).distinct),
      set("derivative", msg.derivative.getOrElse(false)),
      set("derivatives", found.doc.derivatives.getOrElse(List[Derivative]())),
      set("updated", LocalDateTime.now())
    )
    collection.updateOne(equal("_id", found.doc._id), update).toFutureOption()
      .andThen(_ => latency.observeDuration())
      .andThen({
        case Success(_) => downstream.send(DoclibMsg(id = found.doc._id.toString))
        case Failure(e) => throw e
      }).flatMap({
        case Some(_) =>
          collection.find(equal("_id", found.doc._id)).headOption()
        case None =>
          Future.successful(None)
      })
  }

  /**
   * build consolidated list of origins from doc and msg
   *
   * @param found FoundDoc
   * @param msg   PrefetchMsg
   * @return
   */
  def consolidateOrigins(found: FoundDoc, msg: PrefetchMsg): List[Origin] = (
    found.doc.origin.getOrElse(List[Origin]()) :::
      found.origins :::
      msg.origins.getOrElse(List[Origin]())
    ).distinct

  /**
   * moves a file on the file system from its source path to an new root location maintaining the path and prefixing the filename
   *
   * @param source current relative source path
   * @param target relative target path to move file to
   * @return
   */
  def moveFile(source: String, target: String): Option[Path] = moveFile(
    Paths.get(s"$doclibRoot$source").toAbsolutePath.toFile,
    Paths.get(s"$doclibRoot$target").toAbsolutePath.toFile
  ) match {
    case Success(_) => Some(Paths.get(target))
    case Failure(err) => throw err
  }

  /**
   * moves a file on the file system from its source path to an new root location maintaining the path and prefixing the filename
   *
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
        val latency = fileOperationLatency.labels("move").startTimer()
        val path = Files.move(source.toPath, target.toPath, StandardCopyOption.REPLACE_EXISTING)
        latency.observeDuration()
        path
      }
    })
  }

  /**
   * Copies a file to a new location
   *
   * @param source source path
   * @param target target path
   * @return
   */
  def copyFile(source: String, target: String): Option[Path] = copyFile(
    Paths.get(s"$doclibRoot$source").toAbsolutePath.toFile,
    Paths.get(s"$doclibRoot$target").toAbsolutePath.toFile
  ) match {
    case Success(_) => Some(Paths.get(target))
    case Failure(_: FileNotFoundException) => None
    case Failure(err) => throw err
  }

  /**
   * Copies a file to a new location
   *
   * @param source source path
   * @param target target path
   * @return
   */
  def copyFile(source: File, target: File): Try[Path] =
    Try({
      target.getParentFile.mkdirs
      val latency = fileOperationLatency.labels("copy").startTimer()
      val path = Files.copy(source.toPath, target.toPath, StandardCopyOption.REPLACE_EXISTING)
      latency.observeDuration()
      path
    })

  /**
   * Silently remove file and empty parent dirs
   *
   * @param source String
   */
  def removeFile(source: String): Unit = {
    val file = Paths.get(s"$doclibRoot$source").toAbsolutePath.toFile
    val latency = fileOperationLatency.labels("remove").startTimer()
    removeFile(file)
    latency.observeDuration()
  }


  /**
   * Silently remove file and empty parent dirs
   *
   * @param file File
   */
  def removeFile(file: File): Unit = {

    @tailrec
    def remove(o: Option[File]): Unit = {
      o match {
        case Some(f) if f.isFile || Option(f.listFiles()).exists(_.isEmpty) =>
          file.delete()
          remove(Option(f.getParentFile))
        case _ => ()
      }
    }

    remove(Option(file))
  }

  /**
   * tests if source string starts with the configured remote target-dir
   *
   * @param source String
   * @return
   */
  def inRemoteRoot(source: String): Boolean = source.startsWith(s"$remoteDirName/")

  /**
   * tests if source string starts with the configured local target-dir
   *
   * @param source String
   * @return
   */
  def inLocalRoot(source: String): Boolean = source.startsWith(s"$localDirName/")

  /**
   * Tests if found Document currently in the remote root and is not returns the appropriate download target
   *
   * @param foundDoc Found Document and remote data
   * @return
   */
  def getRemoteUpdateTargetPath(foundDoc: FoundDoc): Option[String] =
    if (inRemoteRoot(foundDoc.doc.source))
      Some(Paths.get(s"${foundDoc.doc.source}").toString)
    else
      Some(Paths.get(s"${foundDoc.download.get.target.get}").toString.replaceFirst(s"^$doclibRoot/*", ""))

  /**
   * determines appropriate local target path if required
   *
   * @param foundDoc Found Doc
   * @return
   */
  def getLocalUpdateTargetPath(foundDoc: FoundDoc): Option[String] =
    if (inLocalRoot(foundDoc.doc.source))
      Some(Paths.get(s"${foundDoc.doc.source}").toString)
    else {
      // strips temp dir if present plus any prefixed slashes
      val relPath = foundDoc.doc.source.replaceFirst(s"^$doclibRoot/*", "")
      Some(Paths.get(getTargetPath(relPath, localDirName)).toString)
    }

  def getRemoteOrigins(origins: List[Origin]): List[Origin] = origins.filter(o => {
    Ftp.protocols.contains(o.scheme) || Http.protocols.contains(o.scheme)
  })

  def getLocalToRemoteTargetUpdatePath(origin: Origin): FoundDoc => Option[String] = {
    def getTargetPath(foundDoc: FoundDoc): Option[String] =
      if (inRemoteRoot(foundDoc.doc.source))
        Some(Paths.get(s"${foundDoc.doc.source}").toString)
      else {
        val remotePath = Http.generateFilePath(origin, Option(remoteDirName), None, None)
        Some(Paths.get(s"$remotePath").toString)
      }

    getTargetPath
  }

  /**
   *
   * @param foundDoc      document to be archived
   * @param archiveSource string file to copy
   * @param archiveTarget string location to archive the document to
   * @return
   */
  def archiveDocument(foundDoc: FoundDoc, archiveSource: String, archiveTarget: String): Future[Option[Path]] = {
    logger.info(s"Archive ${foundDoc.archiveable.map(d => d._id).mkString(",")} source=$archiveSource target=$archiveTarget")
    (for {
      archivePath: Path <- OptionT.fromOption[Future](copyFile(archiveSource, archiveTarget))
      _ <- OptionT.liftF(sendDocumentsToArchiver(foundDoc.archiveable))
    } yield archivePath).value
  }

  /**
   * sends documents to archiver for processing.
   *
   * @todo send errors to queue without killing the rest of the process
   */
  def sendDocumentsToArchiver(docs: List[DoclibDoc]): Future[Unit] = {
    val messages =
      for {
        doc <- docs
        id = doc._id.toHexString
      } yield DoclibMsg(id)

    Try(messages.foreach(msg => archiver.send(msg))) match {
        case Success(_) =>
          logger.info(s"Sent documents to archiver: ${messages.map(d => d.id).mkString(",")}")
          Future.successful(())
        case Failure(e) =>
          logger.error("failed to send doc to archive", e)
          Future.successful(())
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
    archiveDocument(foundDoc, targetSource, archive).map(_ => moveFile(temp, targetSource))
  }

  /**
   * generate an archive for the found document
   *
   * @param targetPath the found doc
   * @return
   */
  def getArchivePath(targetPath: String, hash: String): String = {
    val withExt = """(.*)/(.*)\.(.+)$""".r
    val withoutExt = """(.*)/(.*)$""".r

    targetPath match {
      case withExt(path, file, ext) => s"${getTargetPath(path, archiveDirName)}/$file.$ext/$hash.$ext"
      case withoutExt(path, file) => s"${getTargetPath(path, archiveDirName)}/$file/$hash"
      case _ => throw new RuntimeException("Unable to identify path and filename")
    }
  }

  /**
   * Handles the potential update of a document and is associated file based on supplied properties
   *
   * @param foundDoc            the found document
   * @param tempPath            the path of the temporary file either remote or local
   * @param targetPathGenerator function to generate the absolute target path for the file
   * @param inRightLocation     function to test if the current document source path is in the right location
   * @return
   */
  def handleFileUpdate(foundDoc: FoundDoc, tempPath: String, targetPathGenerator: FoundDoc => Option[String], inRightLocation: String => Boolean): Option[Path] = {
    if (!zeroLength(tempPath)) {
      val docHash: String = foundDoc.doc.hash
      targetPathGenerator(foundDoc) match {
        case Some(targetPath) =>
          val absTargetPath = Paths.get(s"$doclibRoot$targetPath").toAbsolutePath
          val currentHash = if (absTargetPath.toFile.exists()) md5(absTargetPath.toFile) else docHash
          if (docHash != currentHash) {
            // file already exists at target location but is not the same file, archive the old one then add the new one
            val archivePath = getArchivePath(targetPath, currentHash)
            Await.result(updateFile(foundDoc, tempPath, archivePath, Some(targetPath)), Duration.Inf)
          } else if (!inRightLocation(foundDoc.doc.source)) {
            moveFile(tempPath, targetPath)
          } else { // not a new file or a file that requires updating so we will just cleanup the temp file
            removeFile(tempPath)
            None
          }
        case None => None
      }
    } else {
      throw new ZeroLengthFileException(tempPath, foundDoc.doc)
    }
  }

  /**
   * Builds a document update with updates source and origins
   *
   * @param foundDoc FoundDoc
   * @param msg      PrefetchMsg
   * @return Bson
   */
  def getDocumentUpdate(foundDoc: FoundDoc, msg: PrefetchMsg): Bson = {
    val currentOrigins: List[Origin] = consolidateOrigins(foundDoc, msg)
    val (source: Option[Path], origin: List[Origin]) = foundDoc.download match {
      case Some(downloaded) =>
        val source = handleFileUpdate(foundDoc, downloaded.source, getRemoteUpdateTargetPath, inRemoteRoot)
        val filteredDocOrigin = currentOrigins.filterNot(d => foundDoc.origins.map(_.uri.toString).contains(d.uri.toString))
        (source, foundDoc.origins ::: filteredDocOrigin)

      case None =>
        val remoteOrigins = getRemoteOrigins(currentOrigins)

        val source =
          remoteOrigins match {
            case origin :: _ =>
              // has at least one remote origin and needs relocating to remote folder
              handleFileUpdate(foundDoc, msg.source, getLocalToRemoteTargetUpdatePath(origin), inRemoteRoot)
            case _ =>
              // does not need remapping to remote location
              handleFileUpdate(foundDoc, msg.source, getLocalUpdateTargetPath, inLocalRoot)
          }

        (source, currentOrigins)
    }
    // source needs to be relative path from doclib.root
    val pathNormalisedSource = {
      val rawPath =
        source match {
          case Some(path: Path) => path.toString
          case None => foundDoc.doc.source
        }
      val root = config.getString("doclib.root")

      rawPath.replaceFirst(s"^$root", "")
    }

    val uuidAssignment =
      foundDoc.doc.uuid match {
        case None =>
          List(set("uuid", UUID.randomUUID()))
        case _ =>
          List()
      }

    val changes = List(
      set("source", pathNormalisedSource),
      set("origin", origin),
      getFileAttrs(source),
      getMimetype(source)
    )

    combine(
      uuidAssignment ::: changes :_*
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
      case Some(path) =>
        val absPath = (doclibRoot / path.toString).path
        val attrs = Files.getFileAttributeView(absPath, classOf[BasicFileAttributeView]).readAttributes()
        set("attrs", FileAttrs(
          path = absPath.getParent.toAbsolutePath.toString,
          name = absPath.getFileName.toString,
          mtime = LocalDateTime.ofInstant(attrs.lastModifiedTime().toInstant, ZoneOffset.UTC),
          ctime = LocalDateTime.ofInstant(attrs.creationTime().toInstant, ZoneOffset.UTC),
          atime = LocalDateTime.ofInstant(attrs.lastAccessTime().toInstant, ZoneOffset.UTC),
          size = attrs.size()
        ))
      case None => combine()
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
      case Some(path) =>
        val absPath = (doclibRoot / path.toString).path
        val metadata = new Metadata()
        metadata.set(TikaMetadataKeys.RESOURCE_NAME_KEY, absPath.getFileName.toString)
        set("mimetype", tika.getDetector.detect(
          TikaInputStream.get(new FileInputStream(absPath.toString)),
          metadata
        ).toString)
      case None => combine()
    }

  /**
   * retrieves a document based on url or filepath
   *
   * @param URI PrefetchUri
   * @return
   */
  def findDocument(URI: PrefetchUri): Future[Option[FoundDoc]] =
    URI.uri match {
      case Some(uri) =>
        uri.schemeOption match {
          case None => throw new UndefinedSchemeException(uri)
          case Some("file") => findLocalDocument(URI.raw)
          case _ => findRemoteDocument(uri)
        }
      case None =>
        findLocalDocument(URI.raw)
    }


  /**
   * retrieves document from mongo based on supplied uri being for a local source
   *
   * @param source String
   * @return
   */
  def findLocalDocument(source: String): Future[Option[FoundDoc]] =
    (for {
      target: String <- OptionT.some[Future](source.replaceFirst(
        s"^${config.getString("doclib.local.temp-dir")}",
        localDirName
      ))
      md5 <- OptionT.some[Future](md5(Paths.get(s"$doclibRoot$source").toFile))
      (doc, archivable) <- OptionT(findOrCreateDoc(source, md5, Some(or(
        equal("source", source),
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
    (
      for { // assumes remote
        origins: List[Origin] <- OptionT.liftF(remoteClient.resolve(uri))
        origin = origins.head
        originUri = origin.uri.get
        downloaded: DownloadResult <- OptionT.fromOption[Future](remoteClient.download(origin))
        (doc, archivable) <- OptionT(
          findOrCreateDoc(
            originUri.toString,
            downloaded.hash,
            Some(
              or(
                equal("origin.uri", originUri.toString),
                equal("source", originUri.toString)
              )
            )
          )
        )
            } yield FoundDoc(doc, archivable, origins = origins, download = Option(downloaded))
      ).value


  /**
   * retrieves document from mongo if no document found will create and persist new document
   *
   * @param source String
   * @param hash   String
   * @return
   */
  def findOrCreateDoc(source: String, hash: String, query: Option[Bson] = None): Future[Option[(DoclibDoc, List[DoclibDoc])]] = {
    collection.find(
        or(
          equal("hash", hash),
          query.getOrElse(combine())
        )
      )
      .sort(descending("created"))
      .toFuture()
      .flatMap({
        case latest :: archivable if latest.hash == hash =>
          Future.successful(latest -> archivable)
        case archivable =>
          createDoc(source, hash).map(doc => doc -> archivable.toList)
      })
      .map(Option.apply)
  }

  def createDoc(source: String, hash: String): Future[DoclibDoc] = {
    val createdInstant = LocalDateTime.now().toInstant(ZoneOffset.UTC)
    val createdTime = LocalDateTime.ofInstant(createdInstant, ZoneOffset.UTC)
    val latency = mongoLatency.labels(consumerName, "insert_document").startTimer()
    val newDoc = DoclibDoc(
      _id = new ObjectId(),
      source = source,
      hash = hash,
      derivative = false,
      created = createdTime,
      updated = createdTime,
      mimetype = "",
      tags = Some(List[String]()),
      uuid = Some(UUID.randomUUID())
    )

    val inserted: Future[Option[InsertOneResult]] =
      collection.insertOne(newDoc).toFutureOption().andThen(_ => latency.observeDuration())

    inserted.map(_ => newDoc)
  }

  /**
   * wraps supplied string in a PrefetchUri object with an optional Lemonlabs Uri property
   *
   * If unable to convert the url or no scheme/protocol identified then assumes file path and not URL
   *
   * @param source String
   * @return
   */
  def toUri(source: String): PrefetchUri = {
    Uri.parseTry(source) match {
      case Success(uri) => uri.schemeOption match {
        case Some(_) => PrefetchUri(source, Some(uri))
        case None => PrefetchUri(source, Some(uri.withScheme("file")))
      }
      case Failure(_) => PrefetchUri(source, None)
    }
  }

  /**
   * function to perform validation of document before processing
   *
   * Checks if just verifying document and if true tests to ensure is not a new doc by checking it is more than 10
   * seconds old (prefetch.verificationTimeout).
   * If it is an old doc, and verifying, then throw SilentValidationException.
   *
   * @param msg      PrefetchMsg
   * @param foundDoc FoundDoc
   * @return
   */
  def valid(msg: PrefetchMsg, foundDoc: FoundDoc): Boolean = {
    val timeSinceCreated = Math.abs(java.time.Duration.between(foundDoc.doc.created, LocalDateTime.now()).getSeconds)

    if (msg.verify.getOrElse(false) && (timeSinceCreated > config.getInt("prefetch.verificationTimeout")))
      throw new SilentValidationException(foundDoc.doc)

    val origins = msg.origins.getOrElse(List())

    origins.forall(origin =>
      if (Ftp.protocols.contains(origin.scheme) || Http.protocols.contains(origin.scheme)) {
        origin.uri match {
          case None =>
            throw new MissingOriginSchemeException(msg, origin)
          case Some(x) if x.schemeOption.isEmpty =>
            throw new InvalidOriginSchemeException(msg)
          case Some(_) =>
            true
        }
      } else {
        // mongodb or something else that we probably don't care too much about
        true
      }
    )
  }

}
