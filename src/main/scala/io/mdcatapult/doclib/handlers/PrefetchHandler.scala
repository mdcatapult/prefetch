package io.mdcatapult.doclib.handlers

import java.io.{File, FileInputStream}
import java.nio.file.attribute.BasicFileAttributeView
import java.nio.file.{Files, Path, Paths, StandardCopyOption}
import java.time.{LocalDateTime, ZoneOffset}
import java.util.Date

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
import scala.util.{Failure, Success}

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
                     ) extends LazyLogging with FileHash with TargetPath{

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
        case Some(found) ⇒ flags.error(found.doc, true)
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
    val (remoteUpdate, source) = fetchRemoteUpdate(found, msg)

    val update = combine(
      remoteUpdate,
      fetchFileAttrs(source),
      fetchMimetype(source),
      fetchFileHash(source),
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
  def consolidateOrigins(doc: Document, msg: PrefetchMsg): List[PrefetchOrigin] = (doc.get[BsonArray]("origin").getOrElse(BsonArray()).getValues.asScala
    .flatMap({
      case d: BsonValue ⇒ Json.parse(d.asDocument().toJson).validate[PrefetchOrigin] match {
        case JsSuccess(value, _) ⇒ Some(value)
        case _ ⇒ None
      }
      case _ ⇒ None
    }).toList ::: msg.origin.getOrElse(List[PrefetchOrigin]())).distinct


  /**
    * moves a file on the file system from its source path to an new root location maintaining the path and prefixing the filename
    * @param source current source path
    * @param target target path to move file to
    * @return
    */
  def moveFile(source: String, target: String): Path = {
    if (source == target) {
      Paths.get(target)
    } else {
      new File(target).getParentFile.mkdirs
      Files.move(Paths.get(source), Paths.get(target), StandardCopyOption.REPLACE_EXISTING)
    }
  }

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
    * Silently remove file and empty parent dirs
    * @param source String
    */
  def removeFile(source:String): Unit = removeFile(new File(source))

  /**
    * archives a document to the archive collection and moves file into archive folder
    * @param doc Source Document
    * @return
    */
  def archiveDoc(doc: Document, move: Boolean = true): Future[Option[Completed]] = {
    val currentPath = Paths.get(doc.getString("source"))
    val archiveDir = getTargetPath(currentPath.toString, new File(config.getString("prefetch.archive.target-dir")).getAbsolutePath.toString)
    val archivePath = s"$archiveDir${doc.getString("hash")}_${currentPath.getFileName.toString}"

    archiveCollection.insertOne(Document(
      "_id" → new ObjectId(),
      "created" → BsonDateTime(LocalDateTime.now().toInstant(ZoneOffset.UTC).toEpochMilli),
      "archive" → BsonString({
        if (move)
          moveFile(doc.getString("source"), archivePath).toString
        else
          currentPath.toString
      }),
      "document" → doc
    )).toFutureOption()
  }

  /**
    * tests if source string starts with the configured remote target-dir
    * @param source String
    * @return
    */
  def inRemoteRoot(source: String): Boolean = source.startsWith(new File(config.getString("prefetch.remote.target-dir")).getAbsolutePath.toString)

  /**
    * Tests if found Document currently in the remote root and is not returns the appropriate download target
    * @param foundDoc
    * @return
    */
  def getRemoteUpdateTargetPath(foundDoc: FoundDoc): Option[String] =
    if (inRemoteRoot(foundDoc.doc.getString("source")))
      Some(foundDoc.doc.getString("source"))
    else
      foundDoc.download.get.target

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
  def fetchRemoteUpdate(foundDoc: FoundDoc, msg: PrefetchMsg): (Bson, String) = {
    val currentOrigins: List[PrefetchOrigin] = consolidateOrigins(foundDoc.doc, msg)
    val currentHash: Option[BsonString] = foundDoc.doc.get[BsonString]("hash")

    foundDoc.download match {
      case Some(downloaded) ⇒
        val source: String = Await.result(
            if (currentHash.nonEmpty && currentHash.get.getValue != downloaded.hash) {
              (for {
                _ ← OptionT(archiveDoc(foundDoc.doc)) // archive file
                target: String ← OptionT.fromOption[Future](getRemoteUpdateTargetPath(foundDoc))
                path ← OptionT.some[Future](moveFile(downloaded.source, target))
                _ ← OptionT.some[Future](removeFile(downloaded.source))
              } yield path).value.map({
                case Some(path) ⇒ Some(path.toString)
                case None ⇒ None
              })
            } else if (currentHash.nonEmpty && !inRemoteRoot(foundDoc.doc.getString("source"))) {
              // ok, so its not a new file but lets make sure that its in the remote files root to ensure its cleaned up
              val newSource = moveFile(
                foundDoc.doc.getString("source"),
                getRemoteUpdateTargetPath(foundDoc).get)
              removeFile(foundDoc.doc.getString("source"))
              Future.successful(Some(newSource.toString))
            } else if (currentHash.isEmpty) {
              // completely new remote file being added to lets move it out of its temp folder
              Future.successful(Some(moveFile(
                downloaded.source,
                downloaded.target.get
              ).toString))
            } else Future.successful(None),
            Duration.Inf
          ).getOrElse(foundDoc.doc.getString("source"))

        val filteredDocOrigin = currentOrigins.filterNot(d ⇒ d.uri.toString == foundDoc.origin.get.uri.toString)
        (combine(
          set("source", source),
          set("origin", foundDoc.origin.get :: filteredDocOrigin)
        ), source)
      case None ⇒
        // its not a remote/downloaded file so lets check how things stand locally
        if (currentHash.nonEmpty && currentHash.get.getValue != md5(foundDoc.doc.getString("source"))) {
          Await.result(archiveDoc(foundDoc.doc, false), Duration.Inf)
        }
        (combine(set("origin", currentOrigins)), foundDoc.doc.getString("source"))
    }
  }

  /**
    * builds a mongo update based on the target files attributes
    *
    * @param source String
    * @return Bson $set
    */
  def fetchFileAttrs(source: String): Bson = {
    val filePath = Paths.get(source)
    val attrs = Files.getFileAttributeView(filePath, classOf[BasicFileAttributeView]).readAttributes()
    set(config.getString("prefetch.labels.attrs"), FileAttrs(
      path = filePath.getParent.toAbsolutePath.toString,
      name = filePath.getFileName.toString,
      mtime = LocalDateTime.ofInstant(attrs.lastModifiedTime().toInstant, ZoneOffset.UTC),
      ctime = LocalDateTime.ofInstant(attrs.creationTime().toInstant, ZoneOffset.UTC),
      atime = LocalDateTime.ofInstant(attrs.lastAccessTime().toInstant, ZoneOffset.UTC),
      size = attrs.size()
    ))
  }

  /**
    * detect mimetype of source file
    *
    * @param source String
    * @return Bson $set
    */
  def fetchMimetype(source: String): Bson = {
    val metadata = new Metadata()
    metadata.set(TikaMetadataKeys.RESOURCE_NAME_KEY, Paths.get(source).getFileName.toString)
    set(config.getString("prefetch.labels.mimetype"), tika.getDetector.detect(
      TikaInputStream.get(new FileInputStream(source)),
      metadata
    ).toString)
  }

  /**
    * generate md5 hash of file
    *
    * @param source String
    * @return Bson $set
    */
  def fetchFileHash(source: String): Bson = set(config.getString("prefetch.labels.hash"), md5(source))



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
        doc: Document ← OptionT(findOrCreateDoc(
          or(
            equal("source", uri.path.toString),
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
