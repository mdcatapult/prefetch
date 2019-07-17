package io.mdcatapult.doclib.handlers

import java.io.{File, FileInputStream}
import java.nio.file.attribute.BasicFileAttributeView
import java.nio.file.{Files, Paths}
import java.security.{DigestInputStream, MessageDigest}
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
import io.mdcatapult.doclib.models.{FileAttrs, PrefetchOrigin}
import io.mdcatapult.doclib.remote.{DownloadResult, UndefinedSchemeException, Client ⇒ RemoteClient}
import io.mdcatapult.klein.queue.Queue
import org.apache.tika.Tika
import org.apache.tika.io.TikaInputStream
import org.apache.tika.metadata.{Metadata, TikaMetadataKeys}
import org.mongodb.scala.bson.conversions.Bson
import org.mongodb.scala.bson.{BsonArray, BsonValue, ObjectId}
import org.mongodb.scala.model.Filters.{equal, or}
import org.mongodb.scala.model.Updates.{addToSet, combine, set}
import org.mongodb.scala.{Document, MongoCollection}
import play.api.libs.json.{JsSuccess, Json}

import scala.collection.JavaConverters._
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContextExecutor, Future}
import scala.util.{Failure, Success}

class PrefetchHandler(downstream: Queue[DoclibMsg])
                     (implicit ac: ActorSystem,
                      materializer: ActorMaterializer,
                      ex: ExecutionContextExecutor,
                      config: Config,
                      collection: MongoCollection[Document]
                     ) extends LazyLogging{

  /** Initialise Apache Tika && Remote Client **/
  lazy val tika = new Tika()
  lazy val remoteClient = new RemoteClient()

  /**
    * handle msg from rabbitmq
    *
    * @param msg PrefetchMsg
    * @param key String
    * @return
    */
  def handle(msg: PrefetchMsg, key: String): Future[Option[Any]] = (for {
    (doc, remoteResponse) ← OptionT(fetchDocument(toUri(msg.source)))
    result ← OptionT(process(doc.get, msg, remoteResponse))
  } yield result).value

  /**
    * process the request and update the target document
    *
    * @param doc            Document
    * @param msg            PrefetchMsg
    * @param remoteResponse Option[PrefetchOrigin]
    * @return
    */
  def process(doc: Document, msg: PrefetchMsg, remoteResponse: Option[PrefetchOrigin] = None): Future[Option[Any]] = {
    val (remoteUpdate, downloaded) = fetchRemote(remoteResponse, doc, msg)

    val source = downloaded match {
      case None ⇒ doc.getString("source")
      case Some(result) ⇒ result.source
    }

    val update = combine(
      remoteUpdate,
      fetchFileAttrs(source),
      fetchMimetype(source),
      fetchFileHash(source),
      addToSet("tags", msg.tags.getOrElse(List[String]()).distinct),
      set("metadata", msg.metadata.getOrElse(Map[String, Any]())),
      set("updated", LocalDateTime.now())
    )

    collection.updateOne(equal("_id", doc.getObjectId("_id")), update).toFutureOption().andThen({
      case Success(_) ⇒ downstream.send(DoclibMsg(id = doc.getObjectId("_id").toString))
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
    * retrieves and persists remote file to target filesystem
    *
    * @param remote Option[PrefetchOrigin]
    * @param msg    PrefetchMsg
    * @return Bson $set for origin & source
    */
  def fetchRemote(remote: Option[PrefetchOrigin], doc: Document, msg: PrefetchMsg): (Bson, Option[DownloadResult]) = {
    val currentOrigins: List[PrefetchOrigin] = consolidateOrigins(doc, msg)
    remote match {
      case Some(response) ⇒ remoteClient.download(response.uri.get) match {
        case Some(result) ⇒
          val filteredDocOrigin = currentOrigins.filterNot(d ⇒ d.uri.toString == response.uri.toString)
          (combine(
            set("source", result.source),
            set("origin", response :: filteredDocOrigin)
          ), Some(result))
        case None ⇒ (combine(set("origin", currentOrigins)), None)
      }
      case None ⇒ (combine(set("origin", currentOrigins)), None)
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
  def fetchFileHash(source: String): Bson = {
    val buffer = new Array[Byte](8192)
    val md5 = MessageDigest.getInstance("MD5")
    val dis = new DigestInputStream(new FileInputStream(new File(source)), md5)
    try {
      while (dis.read(buffer) != -1) {}
    } finally {
      dis.close()
    }
    set(config.getString("prefetch.labels.flags"), md5.digest.map("%02x".format(_)).mkString)
  }

  /**
    * retrieves a document based on url or filepath
    *
    * @param uri io.lemonlabs.uri.Uri
    * @return
    */
  def fetchDocument(uri: Uri): Future[Option[(Option[Document], Option[PrefetchOrigin])]] =
    uri.schemeOption match {
      case None ⇒ throw new UndefinedSchemeException(uri)
      case Some("file") ⇒ for {
        doc: Option[Document] <- findOrCreateDoc(equal("source", uri.path.toString), uri.path.toString)
      } yield Some((doc, None))
      case _ ⇒ for {
        response: PrefetchOrigin ← remoteClient.resolve(uri)
        doc: Option[Document] ← findOrCreateDoc(
          or(
            equal("origin.uri", response.uri.toString),
            equal("source", response.uri.toString),
          ),
          response.uri.toString)
      } yield Some((doc, Some(response)))
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
          "created" → new Date,
          config.getString("prefetch.labels.flags") → Document())
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
