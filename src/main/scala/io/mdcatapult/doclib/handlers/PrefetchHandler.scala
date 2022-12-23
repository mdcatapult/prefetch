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
import io.mdcatapult.doclib.prefetch.model.DocumentTarget
import io.mdcatapult.doclib.prefetch.model.Exceptions._
import io.mdcatapult.doclib.remote.adapters.{Ftp, Http}
import io.mdcatapult.doclib.remote.{DownloadResult, UndefinedSchemeException, Client => RemoteClient}
import io.mdcatapult.doclib.util.{FileProcessor, PathTransformer}
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

import java.io.{FileInputStream, FileNotFoundException}
import java.nio.file.attribute.BasicFileAttributeView
import java.nio.file.{Files, Path, Paths}
import java.time.{LocalDateTime, ZoneOffset}
import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

/**
 * Handler to perform prefetch of source supplied in Prefetch Messages.
 *
 * Will identify existing file or create a new entry in the document library.
 *
 * If it is a "remote" file it will attempt to retrieve the file via http/ftp.
 *
 * Files receive an md5 hash of the contents and if there is a difference between
 * hashes of existing and new version of a file it will attempt to archive the old file(s).
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
                      implicit val config: Config,
                      collection: MongoCollection[DoclibDoc],
                      derivativesCollection: MongoCollection[ParentChildMapping],
                      appConfig: AppConfig)
  extends AbstractHandler[PrefetchMsg]
    with TargetPath with PathTransformer {

  /** Initialise Apache Tika && Remote Client * */
  private val tika = new Tika()
  val remoteClient = new RemoteClient()

  private val fileProcessor = new FileProcessor(sharedConfig.doclibRoot)

  private val consumerName = appConfig.name

  val version: Version = Version.fromConfig(config)

  sealed case class PrefetchUri(raw: String, uri: Option[Uri])

  // The result from processing the message and the document which is sent back to the postHandleProcess
  case class PrefetchResult(doclibDoc: DoclibDoc, foundDoc: FoundDoc) extends HandlerResult

  /**
   * Given an incoming message the process flow is approximately the following:
   *
   * 1. Find an existing record in the db for it.
   *
   * 2. OR Create a new record if it is a new document.
   *
   * 3. Move the document to the appropriate place in the doclib file structure and add metadata to the db record.
   *
   * 4. Archive any existing documents for this record
   *
   * @param msg
   * @return
   */
  override def handle(msg: PrefetchMsg): Future[Option[PrefetchResult]] = {

    //TODO investigate why declaring MongoFlagStore outside of this fn causes large numbers DoclibDoc objects on the heap
    val flagContext = new MongoFlagContext(appConfig.name, version, collection, nowUtc)

    val prefetchUri = toUri(msg.source.replaceFirst(s"^$doclibRoot", ""))

    findDocument(prefetchUri, msg.derivative.getOrElse(false)).map {
      case Some(foundDoc) => foundDocumentProcess(msg, foundDoc, flagContext)
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

    // Call the postHandleProcess with the results of processing this found document via the
    // prefetchProcess method
    postHandleProcess(
      documentId = foundDocId,
      handlerResult = prefetchProcess(msg, foundDoc, flagContext),
      flagContext = flagContext,
      supervisorQueue = supervisor,
      collection = collection
    )
  }

  /**
   * Given a db record and a prefetch message for a document process it:
   *
   * 1. Check that it is valid ie that it wasn't created recently.
   *
   * 2. Change the "context" metadata on the db record for the document to started.
   *
   * 3. Figure out where the document came from and where it needs to go. "generateDocumentTargets"
   *
   * 4. Ingress the document. This moves it to the correct place in the file system and overwrites existing version if it exists. "ingressDocument" > "moveNewAndArchiveExisting
   *
   * 5. Get a BSON update for the db record which includes any new origins and the final path for the document. "getDocumentUpdate"
   *
   * 6. Update the db record in "updateDatabaseRecord" and get the updated record as "newDoc".
   *
   * 7. Update the parent-child mappings if the document is a derivative
   *
   * 8. Change the db record context to "finished"
   *
   * 9. Return the original document (ie foundDoc) and the updated document (ie newDoc)
   *
   * @param msg
   * @param foundDoc
   * @param flagContext
   * @return
   */
  def prefetchProcess(msg: PrefetchMsg,
                      foundDoc: FoundDoc,
                      flagContext: MongoFlagContext): Future[Option[PrefetchResult]] = {
    if (flagContext.isRunRecently(foundDoc.doc)) {
      Future.failed(new Exception(s"document: ${foundDoc.doc._id} run too recently")) //TODO is this exception useful?
    } else {
      val prefetchResult = for {
        _ <- OptionT(Future(Option(valid(msg, foundDoc))))
        started: UpdatedResult <- OptionT.liftF(flagContext.start(foundDoc.doc))
        documentTarget = generateDocumentTargets(foundDoc, msg)
        source <- OptionT.liftF(ingressDocument(foundDoc, documentTarget.source, documentTarget.targetPath, documentTarget.correctLocation))
        documentUpdate = getDocumentUpdate(foundDoc, source, documentTarget.origins)
        newDoc <- OptionT(updateDatabaseRecord(foundDoc, msg, documentUpdate))
        _ <- OptionT.liftF(processParent(newDoc, msg))
        _ <- OptionT.liftF(flagContext.end(foundDoc.doc, noCheck = started.modifiedCount > 0))
      } yield PrefetchResult(newDoc, foundDoc)
      prefetchResult.value
    }
  }

  /**
   * Ingestion process moves a document from the ingress or remote-ingress folder into local or remote-ingress.
   * We need to figure out where to relocate it and the process is:
   *
   * 1. Download the document if required.
   *
   * 2. Figure out whether it is remote, local to remote or local and pass back the appropriate methods to do this.
   *
   * 3. Determine all the remote http/ftp origins for this document.
   *
   * 4. After this we should process the document via the processFoundDocument method to get the eventual path in the doclib root for this document
   *
   * There are 3 different possibilities for a document:
   *
   * 1. The document was downloaded through prefetch from http/ftp. Download it into remote-ingress, move it into remote.
   *
   * 2. The document was originally downloaded from http/ftp but then placed into ingress folder for ingest. Prefetch message likely has origin in its metadata.
   *    Move it from ingress into remote-ingress.
   *
   * 3. The document is a non remote file in the ingress folder. Move it from ingress into local.
   *
   * @param foundDoc
   * @param msg
   * @return A Tuple4 which contains the target path for the document, whether the doc is currently in correct location, the source for the document, the origins
   */
  def generateDocumentTargets(foundDoc: FoundDoc, msg: PrefetchMsg): DocumentTarget = {
    val currentOrigins: List[Origin] = consolidateOrigins(foundDoc, msg)

    foundDoc.download match {
      // 1. The doc has been downloaded from http/ftp
      case Some(downloaded) =>
        val filteredDocOrigin = currentOrigins.collect {
          case origin: Origin if origin.uri.isDefined && foundDoc.origins.contains(origin.uri.get) => origin
        }
        DocumentTarget(getRemoteUpdateTargetPath(foundDoc), inRemoteRoot(foundDoc.doc.source), downloaded.source, foundDoc.origins ::: filteredDocOrigin)

      //2. The doc was on the filesystem ie it was not downloaded via prefetch
      case None =>
        val remoteOrigins = getRemoteOrigins(currentOrigins)
        remoteOrigins match {
          case origin :: _ =>
            // 2.1 The file has remote origins ie. It is a file that has been downloaded outside of prefetch via ftp/http but placed in the ingress dir for ingestion.
            // It needs to be relocated to the remote folder, not local
            val pathCheck = getLocalToRemoteTargetUpdatePath(origin) //This guy is annoying since it returns an inner function!
            DocumentTarget(pathCheck(foundDoc), inRemoteRoot(foundDoc.doc.source), msg.source, currentOrigins)
          case _ =>
            // 2.2 Not from remote origin, it is a file in ingress that needs to be moved to local
            DocumentTarget(getLocalUpdateTargetPath(foundDoc), inLocalRoot(foundDoc.doc.source), msg.source, currentOrigins)
        }
    }
  }

  /**
   * 1. Checks that the file to be processed is not zero length.
   *
   * 2. Generate the file path that the document is to be moved to.
   *
   * 3. Start the file move
   *
   * @param foundDoc            the found document
   * @param tempPath            the path of the temporary file either remote or local
   * @param targetPathGenerator function to generate the absolute target path for the file. It can be for remote or local files depending on what
   *                            was in the original message
   * @param inRightLocation     function to test if the current document source path is in the right location
   * @return Path to the document after it has been ingressed.
   */
  def ingressDocument(foundDoc: FoundDoc, tempPath: String, targetPathGenerator: Option[String], inRightLocation: Boolean): Future[Either[Exception, Option[Path]]] = {
    // First check if the file has any length - this has happened in the past
    if (zeroLength(tempPath)) {
      Future.successful(Left(new ZeroLengthFileException(tempPath, foundDoc.doc)))
    } else {
      // Generate the path that the document is to be moved to using the supplied targetPathGenerator
      targetPathGenerator match {
        case Some(targetPath) => {
          moveFile(foundDoc, tempPath, targetPath, inRightLocation)
        }
        // This file should exist so pass the failure up the chain
        case None => Future.successful(Left(new FileNotFoundException(s"This file really should exist ${foundDoc.doc._id}")))
      }
    }
  }

  /**
   * Moves file to the correct place. Uses MD5 hash to test
   * whether a file is different from existing one. There are 4 possible options:
   *
   * 1. It's a new version of a derivative.
   *
   * 2. It's a new version of a "parent" file
   *
   * 3. It's a brand new file
   *
   * 4. It's (probably) a parent or derivative file that already exists
   *
   * @param foundDoc
   * @param tempPath
   * @param targetPath
   * @param inRightLocation Is the file already in the correct final location ie local or remote
   * @return
   */
  def moveFile(foundDoc: FoundDoc, tempPath: String, targetPath: String, inRightLocation: Boolean): Future[Either[Exception, Option[Path]]] = {
    val newHash: String = foundDoc.doc.hash

    val absTargetPath = Paths.get(s"$doclibRoot$targetPath").toAbsolutePath
    val oldHash = if (absTargetPath.toFile.exists()) md5(absTargetPath.toFile) else newHash

    // If the file is a new version of any existing file we overwrite it. Up to v2.3 we used to archive
    // old versions of non-derivative documents
    if (newHash != oldHash) {
      // 1. It already exists at target location but is not the same file.
      // 2. Delete existing file & derivatives
      // 3. Delete existing parent-child derivative mappings
      // 4. Delete existing 'archiveable' docs for the 'same' file
      // 5. Move the 'new' file to the correct place
      // TODO remove ner & occurrences
      for {
        doclibDoc <- foundDoc.archiveable
        _ = fileProcessor.removeFile(doclibDoc.source)
        _ = writeLimiter(collection, "") {_.deleteOne(equal("_id", doclibDoc._id)).toFutureOption()}
        _ = writeLimiter(derivativesCollection, "") {_.deleteMany(equal("parent", doclibDoc._id)).toFutureOption()}
      } yield doclibDoc
      Future.successful(Right(fileProcessor.moveFile(tempPath, targetPath)))
    } else if (!inRightLocation) {
      // It's a new file in ingress or remote-ingress so move it to the correct place
      Future.successful(Right(fileProcessor.moveFile(tempPath, targetPath)))
    } else {
      // Not a new file or a file that requires updating so we will just cleanup the temp file and move on with our lives
      fileProcessor.removeFile(tempPath)
      Future.successful(Right(None))
    }
  }

  /**
   *  Create a db update with latest metadata for source, origins & file attributes. Should occur after the file has
   * been moved to the correct place and previous versions archived
   * @param foundDoc
   * @param source
   * @param origin
   * @return
   */
  def getDocumentUpdate(foundDoc: FoundDoc, source: Either[Exception, Option[Path]], origin: List[Origin]): Bson = {

    // Note that this is not actually used but at one point we nearly switched to UUIDs instead of ObjectIds for DoclibDoc records
    val uuidAssignment: List[Bson] =
      foundDoc.doc.uuid match {
        case None =>
          List(set("uuid", UUID.randomUUID()))
        case _ =>
          List()
      }

    // If the archive process failed then use the foundDoc, otherwise use the path that the document was moved to via the
    // Right(None) means that it is the same file (probably!)
    val archivedPath: Option[Path] = source match {
      case Right(Some(path: Path)) => Some(path)
      case Right(None) => None
      case Left(_) => None
    }
    // Fancy name for remove the full path up to the doclib root which is used in the metadata in the mongo record as the "source"
    val normalisedDoclibPath = source match {
      case Right(Some(path: Path)) => path.toString.replaceFirst(s"^$doclibRoot", "")
      case Right(None) => foundDoc.doc.source
      case Left(_) => foundDoc.doc.source
    }
    val changes = List(
      set("source", normalisedDoclibPath),
      set("origin", origin),
      getFileAttrs(archivedPath),
      getMimetype(archivedPath)
    )

    combine(uuidAssignment ::: changes: _*)
  }

  /**
   * Update the db record for a document with various changes around metadata, origins & path
   *
   * @param found FoundDoc
   * @param msg   PrefetchMsg
   * @return
   */
  def updateDatabaseRecord(found: FoundDoc, msg: PrefetchMsg, bsonChanges: Bson): Future[Option[DoclibDoc]] = {
    // Note: derivatives has to be added since unarchive (and maybe others) expect this to exist in the record
    //TODO: tags and metadata are optional in a doc. addEachToSet fails if they are null. Tags is set to an empty list
    // during the prefetch process. Changed it to 'set' just in case...
    val latency = mongoLatency.labels(consumerName, "update_document").startTimer()
    val update = combine(
      bsonChanges,
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
   * Builds a db update for a record based on the attribute of a file path
   *
   * @param source Path relative path from doclib.root
   * @return Bson $set
   */
  def getFileAttrs(source: Option[Path]): Bson = {
    //Note: This is currently only used for the path for an archived document
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
  def getMimetype(source: Option[Path]): Bson = {
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
  }

  /**
   * retrieves a db record for a document based on url or filepath
   *
   * @param URI PrefetchUri
   * @return
   */
  def findDocument(URI: PrefetchUri, derivative: Boolean = false): Future[Option[FoundDoc]] = {
    // TODO don't throw exceptions, use Either instead
    URI.uri match {
      case Some(uri) =>
        uri.schemeOption match {
          case None => throw new UndefinedSchemeException(uri)
          case Some("file") => findLocalDocument(URI, derivative)
          case _ => findRemoteDocument(uri)
        }
      case None =>
        findLocalDocument(URI, derivative)
    }
  }


  /**
   * Retrieves db record for a "file:" ingested from the local file system
   *
   * @param source PrefetchUri
   * @return
   */
  def findLocalDocument(source: PrefetchUri, derivative: Boolean = false): Future[Option[FoundDoc]] = {
    val rawURI = source.raw
    val foundDoc = for {
      target: String <- OptionT.some[Future](rawURI.replaceFirst(
        s"^${config.getString("doclib.local.temp-dir")}",
        localDirName
      ))
      md5 <- OptionT.some[Future](md5(Paths.get(s"$doclibRoot$rawURI").toFile))
      (doc, archivable) <- OptionT(findOrCreateDoc(rawURI, md5, derivative, Some(or(
        equal("source", rawURI),
        equal("source", target)
      ))))
    } yield FoundDoc(doc, archivable)
    foundDoc.value
  }


  /**
   * Retrieves document from mongo based on supplied uri being for a remote source.
   * The FoundDoc also contains db records for existing versions of the document that need to be archived
   *
   * @param uri io.lemonlabs.uri.Uri
   * @return
   */
  def findRemoteDocument(uri: Uri): Future[Option[FoundDoc]] = {
    val foundDoc = for {
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
    foundDoc.value
  }


  /**
   * 1. Retrieves existing db records for a document from mongo based on the MD5 hash and query params from calling function eg origin.
   *
   * 2. If no db record found will ask createDoc to create and persist a new record
   *
   * 3. The query will also find existing versions of the document and return them as a list of docs to be archived
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
        // Already have this exact document in the db (by matching the MD5 hash) and other versions which we can archive
        case latest :: archivable if latest.hash == hash =>
          Future.successful(latest -> archivable)
        // Have existing versions of this document but not this exact one so create a new record for it
        case archivable =>
          createDoc(source, hash, derivative).map(doc => doc -> archivable.toList)
      })
      .map(Option.apply)
  }

  /**
   * Create a new db record for a document
   * @param source String version of a file path or a URI for a "local" or "remote" document
   * @param hash
   * @param derivative Is it a parent or a child
   * @return
   */
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
   * Wraps supplied string in a PrefetchUri object with an optional Lemonlabs Uri property
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
   * Perform validation of the document in a message via the existing db record metadata
   *
   * 1. If prefetch message is just verifying document then test to ensure is not a new doc (default is more than 10
   * seconds old). If it is an old doc, and verifying, then throw SilentValidationException.
   *
   * 2. Test that the Origin is not Missing for Ftp/Sftp/Https(s)
   *
   * 3. It could be some legacy Orign scheme so just shrug and move on
   *
   * @param msg      PrefetchMsg
   * @param foundDoc FoundDoc
   * @return
   */
  def valid(msg: PrefetchMsg, foundDoc: FoundDoc): Boolean = {
    val timeSinceCreated = Math.abs(java.time.Duration.between(foundDoc.doc.created, LocalDateTime.now()).getSeconds)

    // TODO don't throw exceptions, use Either instead
    if (msg.verify.getOrElse(false) && (timeSinceCreated > config.getInt("prefetch.verificationTimeout")))
      throw new SilentValidationException(foundDoc.doc)

    if (foundDoc.doc.rogueFile.contains(true)) {
      throw new RogueFileException(msg, foundDoc.doc.source)
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

  /**
   * Build consolidated list of origins from doc and msg
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
   * Return http or ftp urls in a list of Origins
   * @param origins
   * @return
   */
  def getRemoteOrigins(origins: List[Origin]): List[Origin] = origins.filter(o => {
    Ftp.protocols.contains(o.scheme) || Http.protocols.contains(o.scheme)
  })

}
