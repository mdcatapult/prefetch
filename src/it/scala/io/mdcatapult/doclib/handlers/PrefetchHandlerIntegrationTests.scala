package io.mdcatapult.doclib.handlers

import java.io.File
import java.nio.file.{Files, Paths}
import java.time.{LocalDateTime, ZoneOffset}

import akka.actor._
import akka.stream.ActorMaterializer
import akka.testkit.{ImplicitSender, TestKit}
import better.files.Dsl.pwd
import better.files.{File => ScalaFile}
import com.mongodb.client.result.UpdateResult
import com.typesafe.config.{Config, ConfigFactory}
import io.lemonlabs.uri.Uri
import io.mdcatapult.doclib.concurrency.SemaphoreLimitedExecution
import io.mdcatapult.doclib.messages.{DoclibMsg, PrefetchMsg}
import io.mdcatapult.doclib.models.metadata.{MetaString, MetaValueUntyped}
import io.mdcatapult.doclib.models.{Derivative, DoclibDoc, Origin}
import io.mdcatapult.doclib.remote.adapters.{Ftp, Http}
import io.mdcatapult.doclib.util.HashUtils.md5
import io.mdcatapult.doclib.util.{DirectoryDelete, MongoCodecs}
import io.mdcatapult.klein.mongo.Mongo
import io.mdcatapult.klein.queue.Sendable
import org.bson.codecs.configuration.CodecRegistry
import org.mongodb.scala.MongoCollection
import org.mongodb.scala.bson.ObjectId
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
      |  root: "$pwd/test/prefetch-test"
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
      |  database: "prefetch_test"
      |  collection: "documents"
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

  private val readLimiter = SemaphoreLimitedExecution.create(config.getInt("mongo.limit.read"))
  private val writeLimiter = SemaphoreLimitedExecution.create(config.getInt("mongo.limit.write"))

  val handler = new PrefetchHandler(downstream, archiver, readLimiter, writeLimiter)

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
      val parentUpdate: Option[UpdateResult] = Await.result(handler.processParent(childDoc, prefetchMsg), 5 seconds)
      assert(parentUpdate.nonEmpty)
      assert(parentUpdate.get.getMatchedCount == 2)
      assert(parentUpdate.get.getModifiedCount == 2)

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
      assert(Files.exists(Paths.get("test/prefetch-test/local/derivatives/raw.txt").toAbsolutePath))
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
  }

    "Moving a file with the same source and target" should {
      "return the original file path" in {
        val path = handler.moveFile(new File("/a/path/to/a/file.txt"), new File("/a/path/to/a/file.txt"))
        assert(path.success.value == new File("/a/path/to/a/file.txt").toPath)
      }
    }

    "A file with a space in the path" should {
      "be found" in {
        val docLocation = "local/test file.txt"

        val origDoc = Await.result(handler.findLocalDocument(docLocation), 5.seconds)
        val fetchedDoc = Await.result(handler.findLocalDocument(docLocation), 5.seconds)

        def docId(d: handler.FoundDoc) = d.doc._id
        def uuid(d: handler.FoundDoc) = d.doc.uuid

        origDoc.map(docId) should be(fetchedDoc.map(docId))
        origDoc.flatMap(uuid) should be(fetchedDoc.flatMap(uuid))

        fetchedDoc.flatMap(uuid).nonEmpty should be(true)
      }
    }

    "A redirected url" should {
      "be persisted in the origin" in {
        val uri = Uri.parse("https://ndownloader.figshare.com/files/3906475")
        Await.result(handler.remoteClient.resolve(uri), 5.seconds) match {
          case canonical :: rest =>
            assert(canonical.uri.get != uri)
            assert(rest.head.uri.get == uri)
          case Nil => fail("no origins found")
        }
      }
    }

    "Multiple urls which redirect to the same url" should {
      "be inserted in the origin as metadata" in {
        val sourceRedirect = "https://github.com/nginx/nginx/raw/master/conf/fastcgi.conf"
        val uriWithRedirect = Uri.parse(sourceRedirect)
        val similarUri = "http://github.com/nginx/nginx/raw/master/conf/fastcgi.conf"
        val similarUriUriWithRedirect = Uri.parse(similarUri)
        val canonicalUri = Uri.parse("https://raw.githubusercontent.com/nginx/nginx/master/conf/fastcgi.conf")

        // create initial document
        val firstDoc = Await.result(handler.findDocument(handler.PrefetchUri(sourceRedirect, Some(uriWithRedirect))), Duration.Inf).get
        val docLibDoc = Await.result(handler.process(firstDoc, PrefetchMsg(uriWithRedirect.toString())), Duration.Inf).get

        docLibDoc.origin.get match {
          case canonical :: rest =>
            assert(canonical.uri.get == canonicalUri)
            assert(rest.head.uri.get == uriWithRedirect)
          case _ => fail("Expected origins to be a list")
        }

        val secondDoc = Await.result(handler.findDocument(handler.PrefetchUri(similarUri, Some(similarUriUriWithRedirect))), Duration.Inf).get
        assert(secondDoc.doc._id == firstDoc.doc._id)

        val updatedDocLibDoc = Await.result(handler.process(secondDoc, PrefetchMsg(uriWithRedirect.toString())), Duration.Inf).get
        assert(updatedDocLibDoc.origin.get.size == 3)

        updatedDocLibDoc.origin.get match {
          case canonical :: rest =>
            assert(canonical.uri.get == canonicalUri)
            assert(rest.head.uri.get == uriWithRedirect)
            assert(rest(1).uri.get == similarUriUriWithRedirect)
          case _ => fail("Expected origins to be a list")
        }

      }
    }


    "Adding the same url to a doc" should {
      // @todo: depends on previous tests output, needs refactor to isolate with test fixture
      "not be be result in a duplicate origins" in {
        val source = "http://github.com/nginx/nginx/raw/master/conf/fastcgi.conf"
        val similarUri = Uri.parse(source)
        val doc = Await.result(handler.findDocument(handler.PrefetchUri(source, Some(similarUri))), Duration.Inf).get
        assert(doc.origins.size == 3)
      }
    }
  "Prefetch handler" can {
    "calculate if a file has zero length " in {
      val source = "ingress/zero_length_file.txt"
      assert(handler.zeroLength(source))
    }
  }

  "Prefetch handler" can {
    "calculate if a file has length greater than zero " in {
      val source = "ingress/non_zero_length_file.txt"
      assert(!handler.zeroLength(source))
    }
  }

  "If a file has zero length it" should {
    "not be processed" in {
      val id = new ObjectId()
      val createdTime = LocalDateTime.now().toInstant(ZoneOffset.UTC)
      val doc = DoclibDoc(
        _id = id,
        source = "ingress/zero_length_file.txt",
        hash = "12345",
        derivative = false,
        derivatives = None,
        created = LocalDateTime.ofInstant(createdTime, ZoneOffset.UTC),
        updated = LocalDateTime.ofInstant(createdTime, ZoneOffset.UTC),
        mimetype = "text/plain",
        tags = Some(List[String]())
      )
      assertThrows[handler.ZeroLengthFileException] {
        handler.handleFileUpdate(handler.FoundDoc(doc), "ingress/zero_length_file.txt", handler.getLocalUpdateTargetPath, handler.inLocalRoot)
      }
    }
  }

  "Processing the same doc with additional metadata" should {
    "add the metadata to the doclib doc" in {
      val createdTime = LocalDateTime.now().toInstant(ZoneOffset.UTC)
      val docId = new ObjectId()
      val doclibDoc = DoclibDoc(
        _id = docId,
        source = "local/metadata-tags-test/file.txt",
        hash = "12345",
        derivative = false,
        derivatives = None,
        created = LocalDateTime.ofInstant(createdTime, ZoneOffset.UTC),
        updated = LocalDateTime.ofInstant(createdTime, ZoneOffset.UTC),
        mimetype = "text/plain",
        tags = Some(List[String]("one")),
        metadata = Some(List(MetaString("key", "value")))
      )
      val origin: List[Origin] = List(
        Origin(
          scheme = "mongodb",
          hostname = None,
          uri = None,
          metadata = Some(List(MetaString("an", "origin"))),
          headers = None
        )
      )
      val metadataMap: List[MetaString] = List(MetaString("key2", "value2"), MetaString("key3", "value3"))
      val extraTags = List("two", "three")
      val result = Await.result(collection.insertOne(doclibDoc).toFutureOption(), 5 seconds)

      assert(result.get.toString == "The operation completed successfully")

      val prefetchMsg: PrefetchMsg = PrefetchMsg("ingress/metadata-tags-test/file.txt", Some(origin), Some(extraTags), Some(metadataMap), Some(false))
      val docUpdate: Option[DoclibDoc] = Await.result(handler.process(handler.FoundDoc(doclibDoc), prefetchMsg), 5 seconds)
      assert(docUpdate.get.metadata.get.toSet == (doclibDoc.metadata.get ::: metadataMap).toSet)
      assert(docUpdate.get.tags.get.toSet == (doclibDoc.tags.get ::: extraTags).toSet)
    }
  }

  "A doc with no tags or metadata" can {
    "have new tags and metadata added" in {
      val createdTime = LocalDateTime.now().toInstant(ZoneOffset.UTC)
      val docId = new ObjectId()
      val doclibDoc = DoclibDoc(
        _id = docId,
        source = "local/metadata-tags-test/file2.txt",
        hash = "12345",
        derivative = false,
        derivatives = None,
        created = LocalDateTime.ofInstant(createdTime, ZoneOffset.UTC),
        updated = LocalDateTime.ofInstant(createdTime, ZoneOffset.UTC),
        mimetype = "text/plain"
      )
      val origin: List[Origin] = List(
        Origin(
          scheme = "mongodb",
          hostname = None,
          uri = None,
          metadata = Some(List(MetaString("an", "origin"))),
          headers = None
        )
      )
      val metadataMap: List[MetaString] = List(MetaString("key", "value"), MetaString("key2", "value2"))
      val extraTags = List("one", "two")
      val result = Await.result(collection.insertOne(doclibDoc).toFutureOption(), 5 seconds)

      assert(result.get.toString == "The operation completed successfully")

      val prefetchMsg: PrefetchMsg = PrefetchMsg("ingress/metadata-tags-test/file2.txt", Some(origin), Some(extraTags), Some(metadataMap), Some(false))
      val docUpdate: Option[DoclibDoc] = Await.result(handler.process(handler.FoundDoc(doclibDoc), prefetchMsg), 5 seconds)
      assert(docUpdate.get.metadata.get.toSet == metadataMap.toSet)
      assert(docUpdate.get.tags.get.toSet == extraTags.toSet)

    }
  }

  "Different zero length files " should {
    "have the same md5" in {
      val sourceFile = "https/path/to/"
      val doclibRoot = config.getString("doclib.root")
      val ingressDir = config.getString("doclib.local.temp-dir")
      val docFile: ScalaFile = ScalaFile(s"$doclibRoot/$ingressDir/$sourceFile/aFile.txt").createFileIfNotExists(createParents = true)
      val docFile2: ScalaFile = ScalaFile(s"$doclibRoot/$ingressDir/$sourceFile/aFile2.txt").createFileIfNotExists(createParents = true)
      val docFile3: ScalaFile = ScalaFile(s"$doclibRoot/$ingressDir/$sourceFile/aFile3.txt").createFileIfNotExists(createParents = true)
      for {
        tempFile <- docFile.toTemporary
        tempFile2 <- docFile2.toTemporary
        tempFile3 <- docFile3.toTemporary

      } {
        val fileHash = md5(tempFile.toJava)
        val fileHash2 = md5(tempFile2.toJava)
        val fileHash3 = md5(tempFile3.toJava)
        fileHash should (equal (fileHash2) and equal (fileHash3))
      }
    }
  }

  "A file with the same contents" should {
    "not create two docs" in {
      val docLocation = "ingress/zero_length_file.txt"
      val docLocation2 = "ingress/zero_length_file2.txt"
      val origDoc = Await.result(handler.findLocalDocument(docLocation), 5.seconds).get
      val fetchedDoc = Await.result(handler.findLocalDocument(docLocation2), 5.seconds).get
      assert(origDoc.doc._id == fetchedDoc.doc._id)
    }
  }

  "A file with the same name but different contents" should {
    "create two docs" in {
      val doclibRoot = config.getString("doclib.root")
      val ingressDir = config.getString("doclib.local.temp-dir")
      val firstDoc = "aFile.txt"
      val secondDoc = "aFile.txt"
      val docFile: ScalaFile = ScalaFile(s"$doclibRoot/$ingressDir/first/$firstDoc").createFileIfNotExists(createParents = true)
      val docFile2: ScalaFile = ScalaFile(s"$doclibRoot/$ingressDir/second/$secondDoc").createFileIfNotExists(createParents = true)
      docFile.appendLine("Some contents")
      docFile2.appendLine("Different contents")
      val origDoc = Await.result(handler.findLocalDocument(Paths.get(ingressDir, "first", firstDoc).toString), 5.seconds).get
      val fetchedDoc = Await.result(handler.findLocalDocument(Paths.get(ingressDir, "second", firstDoc).toString), 5.seconds).get
      assert(origDoc.doc._id != fetchedDoc.doc._id)
    }
  }

  override def beforeAll(): Unit = {
    Await.result(collection.drop().toFuture(), 5.seconds)
    Try {
      Files.createDirectories(Paths.get("test/prefetch-test/ingress/derivatives").toAbsolutePath)
      Files.createDirectories(Paths.get("test/prefetch-test/local").toAbsolutePath)
      Files.createDirectories(Paths.get("test/prefetch-test/ingress/metadata-tags-test").toAbsolutePath)
      Files.copy(Paths.get("test/raw.txt").toAbsolutePath, Paths.get("test/prefetch-test/ingress/derivatives/raw.txt").toAbsolutePath)
      Files.copy(Paths.get("test/raw.txt").toAbsolutePath, Paths.get("test/prefetch-test/local/test file.txt").toAbsolutePath)
      Files.copy(Paths.get("test/non_zero_length_file.txt").toAbsolutePath, Paths.get("test/prefetch-test/ingress/non_zero_length_file.txt").toAbsolutePath)
      Files.copy(Paths.get("test/zero_length_file.txt").toAbsolutePath, Paths.get("test/prefetch-test/ingress/zero_length_file.txt").toAbsolutePath)
      Files.copy(Paths.get("test/zero_length_file.txt").toAbsolutePath, Paths.get("test/prefetch-test/ingress/zero_length_file2.txt").toAbsolutePath)
      Files.copy(Paths.get("test/raw.txt").toAbsolutePath, Paths.get("test/prefetch-test/ingress/metadata-tags-test/file.txt").toAbsolutePath)
      Files.copy(Paths.get("test/raw.txt").toAbsolutePath, Paths.get("test/prefetch-test/ingress/metadata-tags-test/file2.txt").toAbsolutePath)
    }
  }

  override def afterAll(): Unit = {
    Await.result(collection.drop().toFutureOption(), 5.seconds)
    // These may or may not exist but are all removed anyway
    deleteDirectories(List(pwd/"test"/"prefetch-test"))
  }
}