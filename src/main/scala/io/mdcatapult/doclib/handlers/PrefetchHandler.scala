package io.mdcatapult.doclib.handlers

import akka.stream.Materializer
import better.files._
import cats.data.OptionT
import cats.implicits._
import com.typesafe.config.Config
import io.lemonlabs.uri.Uri
import io.mdcatapult.doclib.consumer.HandlerLogStatus.NoDocumentError
import io.mdcatapult.doclib.consumer.{AbstractHandler, Failed, HandlerResult}
import io.mdcatapult.doclib.flag.MongoFlagContext
import io.mdcatapult.doclib.messages.{DoclibMsg, PrefetchMsg, SupervisorMsg}
import io.mdcatapult.doclib.metrics.Metrics._
import io.mdcatapult.doclib.models._
import io.mdcatapult.doclib.models.metadata._
import io.mdcatapult.doclib.path.TargetPath
import io.mdcatapult.doclib.prefetch.model.Exceptions._
import io.mdcatapult.doclib.remote.adapters.{Ftp, Http}
import io.mdcatapult.doclib.remote.{DownloadResult, UndefinedSchemeException, Client => RemoteClient}
import io.mdcatapult.doclib.util.{Archiver, FileConfig, FileProcessor}
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

import java.io.FileInputStream
import java.nio.file.attribute.BasicFileAttributeView
import java.nio.file.{Files, Path, Paths}
import java.time.{LocalDateTime, ZoneOffset}
import java.util.UUID
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success}

/**
 * Handler to perform prefetch of source supplied in Prefetch Messages
 * Will automatically identify file and create a new entry in the document library
 * if it is a "remote" file it will attempt to retrieve the file and ensure consistency
 * all files receive an md5 hash of its contents if there is a detectable difference between
 * hashes it will attempt to archive and update appropriately
 *
 * @param supervisor    downstream queue to push Document Library messages onto
 * @param archiverQueue queue to push all archived documents to
 * @param readLimiter   used to limit number of concurrent reads from Mongo
 * @param writeLimiter  used to limit number of concurrent writes to Mongo
 * @param ec            ExecutionContext
 * @param m             Materializer
 * @param config        Config
 * @param collection    MongoCollection[Document] to read documents from
 */
class PrefetchHandler(supervisor: Sendable[SupervisorMsg],
                      archiverQueue: Sendable[DoclibMsg],
                      val readLimiter: LimitedExecution,
                      val writeLimiter: LimitedExecution)
                     (implicit ec: ExecutionContext,
                      m: Materializer,
                      config: Config,
                      collection: MongoCollection[DoclibDoc],
                      derivativesCollection: MongoCollection[ParentChildMapping],
                      appConfig: AppConfig)
  extends AbstractHandler[PrefetchMsg]
    with TargetPath {


  /** set props for target path generation */

  /** Initialise Apache Tika && Remote Client * */
  private val tika = new Tika()
  val remoteClient = new RemoteClient()

  private val sharedConfig = FileConfig.getSharedConfig(config)
  private val fileProcessor = new FileProcessor(sharedConfig.doclibRoot)

  private val archiver = new Archiver(archiverQueue, fileProcessor)

  private val doclibRoot = sharedConfig.doclibRoot
  private val archiveDirName = sharedConfig.archiveDirName
  private val localDirName = sharedConfig.localDirName
  private val remoteDirName = sharedConfig.remoteDirName

  private val consumerName = appConfig.name

  val version: Version = Version.fromConfig(config)

  sealed case class PrefetchUri(raw: String, uri: Option[Uri])


  case class PrefetchResult(doclibDoc: DoclibDoc, foundDoc: FoundDoc) extends HandlerResult

  override def handle(msg: PrefetchMsg): Future[Option[PrefetchResult]] = {


    println(s"HANDLING!!! ${msg.source}")
    // TODO investigate why declaring MongoFlagStore outside of this fn causes large numbers DoclibDoc objects on the heap
    val flagContext = new MongoFlagContext(appConfig.name, version, collection, nowUtc)

    val prefetchUri = toUri(msg.source.replaceFirst(s"^$doclibRoot", ""))

    findDocument(prefetchUri, msg.derivative.getOrElse(false)).map {
      case Some(foundDoc) => foundDocumentProcess(msg, foundDoc, flagContext)
//        println(s"foundDoc ${foundDoc.doc.hash} is rogue: ${foundDoc.doc.rogueFile.contains(true)}")
//        if (!foundDoc.doc.rogueFile.contains(true)) foundDocumentProcess(msg, foundDoc, flagContext)
//        else Future.failed(new Exception(s"file ${foundDoc.doc.source} has been marked as rogue - processing aborted"))

      case None =>
        // if we can't identify a document by a document id, log error
        incrementHandlerCount(NoDocumentError)
        logger.error(s"$Failed - $NoDocumentError, prefetch message source ${msg.source}")
        Future.failed(new Exception(s"no document found for URI: $prefetchUri"))
    }.flatten
  }

  def foundDocumentProcess(msg: PrefetchMsg,
                           foundDoc: FoundDoc,
                           flagContext: MongoFlagContext): Future[Option[PrefetchResult]] = {

    val foundDocId = foundDoc.doc._id.toHexString
    logReceived(foundDocId)

    val prefetchProcess = {
      if (flagContext.isRunRecently(foundDoc.doc)) {
        Future.failed(new Exception(s"document: ${foundDoc.doc._id} run too recently")) //TODO is this exception useful?
      } else {
        (for {
          _ <- OptionT(Future(Option(valid(msg, foundDoc))))
          started: UpdatedResult <- OptionT.liftF(flagContext.start(foundDoc.doc))
          newDoc <- OptionT(process(foundDoc, msg))
          _ <- OptionT.liftF(processParent(newDoc, msg))
          _ <- OptionT.liftF(flagContext.end(foundDoc.doc, noCheck = started.modifiedCount > 0))
        } yield PrefetchResult(newDoc, foundDoc)).value
      }
    }

    postHandleProcess(
      documentId = foundDocId,
      handlerResult = prefetchProcess,
      flagContext = flagContext,
      supervisorQueue = supervisor,
      collection = collection
    )
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

    val filter: Bson = equal("_id", found.doc._id)

    collection.updateOne(filter, update).toFutureOption()
      .andThen(_ => latency.observeDuration())
      .andThen({
        case Failure(e) => throw e
      }).flatMap({
      case Some(_) =>
        collection.find(filter).headOption()
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
   * generate an archive for the found document
   *
   * @param targetPath the found doc
   * @return
   */
  def getArchivePath(targetPath: String, hash: String): String = {
    // withExt will incorrectly match files without extensions if there is a "." in the path.
    val withExt = """^(.+)/([^/]+)\.(.+)$""".r
    val withoutExt = """^(.+)/([^/\.]+)$""".r

    // match against withoutExt first and fall through to withExt
    targetPath match {
      case withoutExt(path, file) => s"${getTargetPath(path, archiveDirName)}/$file/$hash"
      case withExt(path, file, ext) => s"${getTargetPath(path, archiveDirName)}/$file.$ext/$hash.$ext"
      case _ => throw new RuntimeException(s"Unable to identify path and filename for targetPath: $targetPath")
    }
  }

  /**
   * Handles the potential update of a document and its associated file based on supplied properties
   * by archiving, moving, or removing the file.
   *
   * @param foundDoc            the found document
   * @param tempPath            the path of the temporary file either remote or local
   * @param targetPathGenerator function to generate the absolute target path for the file
   * @param inRightLocation     function to test if the current document source path is in the right location
   * @return
   */
  def archiveOrProcess(foundDoc: FoundDoc, tempPath: String, targetPathGenerator: FoundDoc => Option[String], inRightLocation: String => Boolean): Option[Path] = {
    if (zeroLength(tempPath)) {
      throw new ZeroLengthFileException(tempPath, foundDoc.doc)
    } else {
      targetPathGenerator(foundDoc) match {
        case Some(targetPath) =>
          val newHash: String = foundDoc.doc.hash

          val absTargetPath = Paths.get(s"$doclibRoot$targetPath").toAbsolutePath
          val oldHash = if (absTargetPath.toFile.exists()) md5(absTargetPath.toFile) else newHash

          if (newHash != oldHash && foundDoc.doc.derivative) {
            // File already exists at target location but is not the same file.
            // Overwrite it and continue because we don't archive derivatives.
            fileProcessor.moveFile(tempPath, targetPath)
          } else if (newHash != oldHash) {
            // file already exists at target location but is not the same file, archive the old one then add the new one
            val archivePath = getArchivePath(targetPath, oldHash)
            // Infinite await? Do we care if this happens before we move on? We already know the path that gets returned here
            // so why are we waiting for it?
            Await.result(archiver.archiveDocument(foundDoc, tempPath, archivePath, Some(targetPath)), Duration.Inf)
          } else if (!inRightLocation(foundDoc.doc.source)) {
            fileProcessor.moveFile(tempPath, targetPath)
          } else { // not a new file or a file that requires updating so we will just cleanup the temp file
            fileProcessor.removeFile(tempPath)
            None
          }
        case None => None

      }
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
        val source = archiveOrProcess(foundDoc, downloaded.source, getRemoteUpdateTargetPath, inRemoteRoot)
        val filteredDocOrigin = currentOrigins.filterNot(d => foundDoc.origins.map(_.uri.toString).contains(d.uri.toString))
        (source, foundDoc.origins ::: filteredDocOrigin)

      case None =>
        val remoteOrigins = getRemoteOrigins(currentOrigins)

        val source =
          remoteOrigins match {
            case origin :: _ =>
              // has at least one remote origin and needs relocating to remote folder
              archiveOrProcess(foundDoc, msg.source, getLocalToRemoteTargetUpdatePath(origin), inRemoteRoot)
            case _ =>
              // does not need remapping to remote location
              archiveOrProcess(foundDoc, msg.source, getLocalUpdateTargetPath, inLocalRoot)
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
      uuidAssignment ::: changes: _*
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
  def findDocument(URI: PrefetchUri, derivative: Boolean = false): Future[Option[FoundDoc]] =
    URI.uri match {
      case Some(uri) =>
        uri.schemeOption match {
          case None => throw new UndefinedSchemeException(uri)
          case Some("file") => {
            println("finding local!")
            findLocalDocument(URI.raw, derivative)
          }
          case _ => {
            println("finding remote!")
            findRemoteDocument(uri)
          }
        }
      case None =>
        findLocalDocument(URI.raw, derivative)
    }


  /**
   * retrieves document from mongo based on supplied uri being for a local source
   *
   * @param source String
   * @return
   */
  def findLocalDocument(source: String, derivative: Boolean = false): Future[Option[FoundDoc]] =
    (for {
      target: String <- OptionT.some[Future](source.replaceFirst(
        s"^${config.getString("doclib.local.temp-dir")}",
        localDirName
      ))
      md5 <- OptionT.some[Future](md5(Paths.get(s"$doclibRoot$source").toFile))
      (doc, archivable) <- OptionT(findOrCreateDoc(source, md5, derivative, Some(or(
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
        origin: Origin = origins.head
        originUri: Uri = origin.uri.get //TODO .get
        downloaded: Option[DownloadResult] <- OptionT.liftF(remoteClient.download(origin))
        (doc, archivable) <- OptionT(
          findOrCreateDoc(
            originUri.toString,
            downloaded.get.hash,
            false,
            Some(
              or(
                equal("origin.uri", originUri.toString),
                equal("source", originUri.toString)
              )
            )
          )
        )
            } yield FoundDoc(doc, archivable, origins = origins, download = downloaded)
      ).value


  /**
   * retrieves document from mongo if no document found will create and persist new document
   *
   * @param source String
   * @param hash   String
   * @return
   */
  def findOrCreateDoc(source: String, hash: String, derivative: Boolean = false, query: Option[Bson] = None): Future[Option[(DoclibDoc, List[DoclibDoc])]] = {
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
          createDoc(source, hash, derivative).map(doc => doc -> archivable.toList)
      })
      .map(Option.apply)
  }

  def createDoc(source: String, hash: String, derivative: Boolean = false): Future[DoclibDoc] = {
    val createdInstant = LocalDateTime.now().toInstant(ZoneOffset.UTC)
    val createdTime = LocalDateTime.ofInstant(createdInstant, ZoneOffset.UTC)
    val latency = mongoLatency.labels(consumerName, "insert_document").startTimer()
    val newDoc = DoclibDoc(
      _id = new ObjectId(),
      source = source,
      hash = hash,
      derivative = derivative,
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
   *
   * Documents with rogue files will fail validation.
   *
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

    if (foundDoc.doc.rogueFile.contains(true)) {
      throw new Exception(s"file ${foundDoc.doc.source} has been marked as rogue")
    }

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
