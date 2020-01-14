package io.mdcatapult.doclib.handlers

import java.io.File
import java.nio.file.{Files, Paths}
import java.time.{LocalDateTime, ZoneOffset}

import akka.actor._
import akka.stream.ActorMaterializer
import akka.testkit.{ImplicitSender, TestKit}
import better.files.Dsl.pwd
import com.mongodb.client.result.UpdateResult
import com.typesafe.config.{Config, ConfigFactory}
import io.lemonlabs.uri.Uri
import io.mdcatapult.doclib.messages.{DoclibMsg, PrefetchMsg}
import io.mdcatapult.doclib.models.metadata.{MetaString, MetaValueUntyped}
import io.mdcatapult.doclib.models.{Derivative, DoclibDoc, Origin}
import io.mdcatapult.doclib.remote.adapters.{Ftp, Http}
import io.mdcatapult.doclib.util.{DirectoryDelete, MongoCodecs}
import io.mdcatapult.klein.mongo.Mongo
import io.mdcatapult.klein.queue.Sendable
import org.bson.codecs.configuration.CodecRegistry
import org.mongodb.scala.MongoCollection
import org.mongodb.scala.bson.ObjectId
import org.mongodb.scala.model.Filters.{equal ⇒ mequal}
import org.scalamock.scalatest.MockFactory
import org.scalatest.TryValues._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContextExecutor}
import scala.language.postfixOps
import scala.util.Try

class PrefetchHandlerIntegrationTests extends TestKit(ActorSystem("PrefetchHandlerIntegrationTest", ConfigFactory.parseString(
  """
  akka.loggers = ["akka.testkit.TestEventListener"]
  """))) with ImplicitSender
  with WordSpecLike
  with Matchers
  with BeforeAndAfterAll with MockFactory with ScalaFutures with DirectoryDelete {

  implicit val config: Config = ConfigFactory.parseString(
    s"""
      |doclib {
      |  root: "${pwd}/test"
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
      |  derivative {
      |    target-dir: "derivatives"
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
    """.stripMargin).withFallback(ConfigFactory.load())

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

      val origin: List[Origin] = List(Origin(
        scheme = "mongodb",
        hostname = None,
        uri = None,
        metadata = Some(List(MetaString("_id", parentIdOne.toString))),
        headers = None
      ),
        Origin(
          scheme = "mongodb",
          hostname = None,
          uri = None,
          metadata = Some(List(MetaString("_id", parentIdTwo.toString))),
          headers = None)
      )
      val childDoc = DoclibDoc(
      _id = childId,
      source = "ingress/derivatives/remote/http/path/to/unarchived_parent.zip/child.txt",
      hash = "12345",
      derivative = true,
      created = LocalDateTime.ofInstant(createdTime, ZoneOffset.UTC),
      updated = LocalDateTime.ofInstant(createdTime, ZoneOffset.UTC),
      mimetype = "text/plain",
      tags = Some(List[String]()),
      origin = Some(origin)
      )
      val childResult = Await.result(collection.insertOne(childDoc).toFutureOption(), 5 seconds)
      assert(childResult.get.toString == "The operation completed successfully")
      val metadataMap: List[MetaString] = List(MetaString("doi", "10.1101/327015"))
      val prefetchMsg: PrefetchMsg = PrefetchMsg("ingress/derivatives/remote/http/path/to/unarchived_parent.zip/child.txt", None, Some(List("a-tag")), Some(metadataMap), Some(true))
      val parentUpdate: List[Option[UpdateResult]] = Await.result(handler.processParent(childDoc, prefetchMsg), 5 seconds)
;      assert(parentUpdate.length == 2)
      parentUpdate.foreach(r => {
        assert(r.get.getMatchedCount == 1)
        assert(r.get.getModifiedCount == 1)
      })

    }
  }

  "A derivative message" should {
    "cause doc to be updated with derivative true" in {
      val createdTime = LocalDateTime.now().toInstant(ZoneOffset.UTC)
      val parentIdOne = new ObjectId()
      val parentDocOne = DoclibDoc(
        _id = parentIdOne,
        source = "ingress/derivatives/raw.txt",
        hash = "12345",
        derivative = false,
        derivatives = None,
        created = LocalDateTime.ofInstant(createdTime, ZoneOffset.UTC),
        updated = LocalDateTime.ofInstant(createdTime, ZoneOffset.UTC),
        mimetype = "text/plain",
        tags = Some(List[String]())
      )
      val origin: List[Origin] = List(
        Origin(
          scheme = "mongodb",
          hostname = None,
          uri = None,
          metadata = Some(List(MetaString("_id", parentIdOne.toString))),
          headers = None
        )
      )
      val metadataMap: List[MetaString] = List(MetaString("doi", "10.1101/327015"))

      val parentResultOne = Await.result(collection.insertOne(parentDocOne).toFutureOption(), 5 seconds)

      assert(parentResultOne.get.toString == "The operation completed successfully")

      val prefetchMsg: PrefetchMsg = PrefetchMsg("ingress/derivatives/raw.txt", Some(origin), Some(List("a-tag")), Some(metadataMap), Some(true))
      val docUpdate: Option[DoclibDoc] = Await.result(handler.process(handler.FoundDoc(parentDocOne), prefetchMsg), 5 seconds)
      assert(docUpdate.get.derivative)
      assert(Files.exists(Paths.get("test/local/derivatives/raw.txt").toAbsolutePath))
    }
  }

  "An http URI" should {
    "be downloaded by the HTTP adapter" in {
      // Result not actually important just the fact that it triggers the "download" method
      val uri: Uri = Uri.parse("http://a/file/somewhere")
      assertThrows[Exception] {
        Http.unapply(uri)
      }
    }
  }

  "An ftp URI" should {
    "be downloaded by the FTP adapter" in {
      // Result not actually important just the fact that it triggers the "download" method
      val uri: Uri = Uri.parse("ftp://a/file/somewhere")
      assertThrows[Exception] {
        Ftp.unapply(uri)
      }
    }
  }

  "Moving a non existent file" should {
    "throw an exception" in {
      assertThrows[Exception] {
        handler.moveFile("/a/file/that/does/no/exist.txt", "./aFile.txt")
      }
    }

    "Moving a file with the same source and target" should {
      "return the original file path" in {
        val path = handler.moveFile(new File("/a/path/to/a/file.txt"), new File("/a/path/to/a/file.txt"))
        assert(path.success.value == new File("/a/path/to/a/file.txt").toPath)
      }
    }

    "A redirected url" should {
      "be persisted in the origin" in {
        val uri = Uri.parse("https://ndownloader.figshare.com/files/3906475")
        val redirectedUrl = Await.result(handler.remoteClient.resolve(uri), 5.seconds)
        assert(uri != redirectedUrl.uri.get)
        assert(redirectedUrl.metadata.get.filter(p ⇒ p.getKey == "original.uri").head.getValue == uri.toString())
      }
    }

    "Multiple urls which redirect to the same url" should {
      "be inserted in the origin as metadata" in {
        val origUri = Uri.parse("https://github.com/nginx/nginx/raw/master/conf/fastcgi.conf")
        val similarUri = Uri.parse("http://github.com/nginx/nginx/raw/master/conf/fastcgi.conf")

        // create initial document
        val newDoc = Await.result(handler.findDocument(origUri), Duration.Inf).get
        val docLibDoc = Await.result(handler.process(newDoc, PrefetchMsg(origUri.toString())), Duration.Inf).get

        // add the original and redirected origins to the doc
        Await.result(handler.addAllOrigins(docLibDoc, origUri.toString()), 5.seconds)
        val insertedDoc = Await.result(collection.find(mequal("_id", newDoc.doc._id)).toFuture(), 5.seconds).head
        // check that it has the original and redirected origin
        assert(insertedDoc.origin.get.length == 2)
        val foundDoc = Await.result(handler.findDocument(similarUri), Duration.Inf).get
        val processedDoc = Await.result(handler.process(foundDoc, PrefetchMsg(similarUri.toString())), Duration.Inf).get
        // update the origins for the second "original" url
        Await.result(handler.addAllOrigins(processedDoc, similarUri.toString()), 5.seconds)
        val updatedDoc = Await.result(collection.find(mequal("_id", foundDoc.doc._id)).toFuture(), 5.seconds).head
        assert(updatedDoc.origin.get.length == 3)
      }
    }

    "Adding the same url to a doc" should {
      "not be be inserted in the origin as metadata" in {
        val similarUri = Uri.parse("http://github.com/nginx/nginx/raw/master/conf/fastcgi.conf")
        val foundDoc = Await.result(handler.findDocument(similarUri), Duration.Inf).get

        Await.result(handler.addAllOrigins(foundDoc.doc, similarUri.toString()), 5.seconds)
        val updatedDoc = Await.result(collection.find(mequal("_id", foundDoc.doc._id)).toFuture(), 5.seconds).head
        assert(updatedDoc.origin.get.length == 3)
      }
    }

  }

  override def beforeAll(): Unit = {
    Try {
      Files.createDirectories(Paths.get("test/ingress/derivatives").toAbsolutePath)
      Files.copy(Paths.get("test/raw.txt").toAbsolutePath, Paths.get("test/ingress/derivatives/raw.txt").toAbsolutePath)
    }
  }

  override def afterAll(): Unit = {
    Await.result(collection.drop().toFutureOption(), 5.seconds)
    // These may or may not exist but are all removed anyway
    deleteDirectories(List(pwd/"test"/"remote-ingress", pwd/"test"/"remote", pwd/"test"/"ingress", pwd/"test"/"local"/"derivatives", pwd/"test"/"ingress"/"derivatives"))
  }
}