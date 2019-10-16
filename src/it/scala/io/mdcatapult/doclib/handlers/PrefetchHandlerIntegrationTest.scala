package io.mdcatapult.doclib.handlers

import java.time.{LocalDateTime, ZoneOffset}

import akka.actor._
import akka.stream.ActorMaterializer
import akka.testkit.{ImplicitSender, TestKit}
import cats.data.OptionT
import com.typesafe.config.{Config, ConfigFactory}
import io.lemonlabs.uri.Uri
import io.mdcatapult.doclib.messages.{DoclibMsg, PrefetchMsg}
import io.mdcatapult.doclib.models.metadata.{MetaString, MetaValueUntyped}
import io.mdcatapult.doclib.models.{Derivative, DoclibDoc, Origin}
import io.mdcatapult.doclib.util.MongoCodecs
import io.mdcatapult.klein.mongo.Mongo
import io.mdcatapult.klein.queue.Sendable
import org.bson.codecs.configuration.CodecRegistry
import org.mongodb.scala.{Completed, MongoCollection, Observable, Observer}
import org.mongodb.scala.bson.ObjectId
import org.mongodb.scala.model.Filters.{and, exists, equal => mequal}
import org.mongodb.scala.model.Updates._
import com.mongodb.client.result.UpdateResult
import org.scalamock.scalatest.MockFactory
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContextExecutor}
import scala.language.postfixOps
import scala.util.{Failure, Success}

class PrefetchHandlerIntegrationTests extends TestKit(ActorSystem("PrefetchHandlerIntegrationTest", ConfigFactory.parseString(
  """
  akka.loggers = ["akka.testkit.TestEventListener"]
  """))) with ImplicitSender
  with WordSpecLike
  with Matchers
  with BeforeAndAfterAll with MockFactory with ScalaFutures {

  implicit val config: Config = ConfigFactory.parseString(
    """
      |doclib {
      |  root: "./test"
      |  remote {
      |    target-dir: "remote"
      |    temp-dir: "remote-ingress"
      |  }
      |  local {
      |    target-dir: "local"
      |    temp-dir: "ingress"
      |  }
      |  archive {
      |    target-dir: "archive"
      |  }
      |}
      |mongo {
      |  database: "prefetch-test"
      |  collection: "documents"
      |  connection {
      |    username: "doclib"
      |    password: "doclib"
      |    database: "admin"
      |    hosts: ["localhost"]
      |  }
      |}
    """.stripMargin)

  /** Initialise Mongo **/

  implicit val codecs: CodecRegistry = MongoCodecs.get
  val mongo: Mongo = new Mongo()
  implicit val collection: MongoCollection[DoclibDoc] = mongo.database.getCollection(config.getString("mongo.collection"))

  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val executor: ExecutionContextExecutor = scala.concurrent.ExecutionContext.global

  implicit val upstream: Sendable[PrefetchMsg] = stub[Sendable[PrefetchMsg]]
  val downstream: Sendable[DoclibMsg] = stub[Sendable[DoclibMsg]]
  val archiver: Sendable[DoclibMsg] = stub[Sendable[DoclibMsg]]
  val handler = new PrefetchHandler(downstream, archiver)

  "Parent docs" should {
    "be updated wth new child info" in {
      val createdTime = LocalDateTime.now().toInstant(ZoneOffset.UTC)
      val childMetadata: List[MetaValueUntyped] = List[MetaValueUntyped](MetaString("metadata-key", "metadata-value"))
      val derivative: Derivative = new Derivative(`type` = "unarchived", path = "ingress/derivatives/remote/http/path/to/unarchived_parent.zip/child.txt", metadata = Some(childMetadata))
      val newDerivative: Derivative = new Derivative(`type` = "unarchived", path = "blah", metadata = Some(childMetadata))
      val derivatives: List[Derivative] = List[Derivative](derivative)
      val parentIdOne = new ObjectId()
      val parentIdTwo = new ObjectId()
      val childId = new ObjectId()
      val parentDocOne = DoclibDoc(
        _id = parentIdOne,
        source = "remote/http/path/to/parent.zip",
        hash = "12345",
        derivative = false,
        derivatives = Some(derivatives),
        created = LocalDateTime.ofInstant(createdTime, ZoneOffset.UTC),
        updated = LocalDateTime.ofInstant(createdTime, ZoneOffset.UTC),
        mimetype = "text/plain",
        tags = Some(List[String]())
      )
      val parentDocTwo = DoclibDoc(
        _id = parentIdTwo,
        source = "remote/http/path/to/another/parent.zip",
        hash = "67890",
        derivative = false,
        derivatives = Some(derivatives),
        created = LocalDateTime.ofInstant(createdTime, ZoneOffset.UTC),
        updated = LocalDateTime.ofInstant(createdTime, ZoneOffset.UTC),
        mimetype = "text/plain",
        tags = Some(List[String]())
      )
      val parentResultOne = Await.result(collection.insertOne(parentDocOne).toFutureOption(), 5 seconds)
      val parentResultTwo = Await.result(collection.insertOne(parentDocTwo).toFutureOption(), 5 seconds)

      assert(parentResultOne.get.toString == "The operation completed successfully")
      assert(parentResultTwo.get.toString == "The operation completed successfully")

      val childDoc = DoclibDoc(
        _id = childId,
        source = "ingress/derivatives/remote/http/path/to/unarchived_parent.zip/child.txt",
        hash = "12345",
        derivative = false,
        created = LocalDateTime.ofInstant(createdTime, ZoneOffset.UTC),
        updated = LocalDateTime.ofInstant(createdTime, ZoneOffset.UTC),
        mimetype = "text/plain",
        tags = Some(List[String]())
      )
      val childResult = Await.result(collection.insertOne(childDoc).toFutureOption(), 5 seconds)
      assert(childResult.get.toString == "The operation completed successfully")
      val metadataMap: List[MetaString] = List(MetaString("doi", "10.1101/327015"))
      val origin: List[Origin] = List(Origin(
        scheme = "mongodb",
        uri = None,
        metadata = Some(List(MetaString("_id", parentIdOne.toString))),
        headers = None
      ),
        Origin(
          scheme = "mongodb",
          uri = None,
          metadata = Some(List(MetaString("_id", parentIdTwo.toString))),
          headers = None)
      )
      val prefetchMsg: PrefetchMsg = PrefetchMsg("ingress/derivatives/remote/http/path/to/unarchived_parent.zip/child.txt", Some(origin), Some(List("a-tag")), Some(metadataMap), Some(true))
      val parentUpdate: Option[UpdateResult] = Await.result(handler.processParent(prefetchMsg), 5 seconds)
      assert(parentUpdate.get.getMatchedCount == 2)
      assert(parentUpdate.get.getModifiedCount == 2)
    }
  }
}

