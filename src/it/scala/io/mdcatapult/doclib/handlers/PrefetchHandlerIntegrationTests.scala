package io.mdcatapult.doclib.handlers

import akka.actor._
import akka.testkit.{ImplicitSender, TestKit}
import better.files.{File => ScalaFile}
import com.mongodb.client.result.UpdateResult
import com.typesafe.config.ConfigFactory
import io.lemonlabs.uri.Uri
import io.mdcatapult.doclib.messages.PrefetchMsg
import io.mdcatapult.doclib.models.metadata.{MetaString, MetaValueUntyped}
import io.mdcatapult.doclib.models.{DoclibDoc, Origin, ParentChildMapping}
import io.mdcatapult.doclib.prefetch.model.Exceptions.ZeroLengthFileException
import io.mdcatapult.doclib.handlers.PrefetchHandler
import io.mdcatapult.util.hash.Md5.md5
import org.mongodb.scala.bson.ObjectId
import org.mongodb.scala.model.Filters.{and, equal => Mequal}
import org.scalamock.scalatest.MockFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.OptionValues._
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Seconds, Span}

import java.nio.file.{Files, Paths}
import java.time.{LocalDateTime, ZoneOffset}
import java.util.UUID
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.language.postfixOps
import scala.util.Try

class PrefetchHandlerIntegrationTests extends TestKit(ActorSystem("PrefetchHandlerIntegrationTest", ConfigFactory.parseString(
  """
  akka.loggers = ["akka.testkit.TestEventListener"]
  """))) with ImplicitSender
  with AnyFlatSpecLike
  with Matchers
  with BeforeAndAfterAll with MockFactory with ScalaFutures with PrefetchHandlerBaseTest {

  import system.dispatcher

  val handler = new PrefetchHandler(downstream, archiver, readLimiter, writeLimiter)

  "Derivative mappings" should "be updated wth new child info" in {
    val createdTime = LocalDateTime.now().toInstant(ZoneOffset.UTC)
    val childMetadata: List[MetaValueUntyped] = List[MetaValueUntyped](MetaString("metadata-key", "metadata-value"))
    val parentIdOne = new ObjectId()
    val parentIdTwo = new ObjectId()
    val childId = new ObjectId()
    val parentDocOne = DoclibDoc(
      _id = parentIdOne,
      source = "remote/http/path/to/parent.zip",
      hash = "12345",
      derivative = false,
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
      created = LocalDateTime.ofInstant(createdTime, ZoneOffset.UTC),
      updated = LocalDateTime.ofInstant(createdTime, ZoneOffset.UTC),
      mimetype = "text/plain",
      tags = Some(List[String]())
    )
    val parentResultOne = Await.result(collection.insertOne(parentDocOne).toFutureOption(), 5 seconds)
    val parentResultTwo = Await.result(collection.insertOne(parentDocTwo).toFutureOption(), 5 seconds)

    assert(parentResultOne.exists(_.wasAcknowledged()))
    assert(parentResultTwo.exists(_.wasAcknowledged()))

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
      source = "local/derivatives/remote/http/path/to/unarchived_parent.zip/child.txt",
      hash = "12345",
      derivative = true,
      created = LocalDateTime.ofInstant(createdTime, ZoneOffset.UTC),
      updated = LocalDateTime.ofInstant(createdTime, ZoneOffset.UTC),
      mimetype = "text/plain",
      tags = Some(List[String]()),
      origin = Some(origin)
    )
    val childResult = Await.result(collection.insertOne(childDoc).toFutureOption(), 5 seconds)
    assert(childResult.exists(_.wasAcknowledged()))
    val firstMappingId = UUID.randomUUID
    val secondMappingId = UUID.randomUUID
    val parentChildMappingOne = ParentChildMapping(_id = firstMappingId, parent = parentIdOne, child = Some(childId), childPath = "ingress/derivatives/remote/http/path/to/unarchived_parent.zip/child.txt", metadata = Some(childMetadata))
    val parentChildMappingTwo = ParentChildMapping(_id = secondMappingId, parent = parentIdTwo, child = Some(childId), childPath = "ingress/derivatives/remote/http/path/to/unarchived_parent.zip/child.txt", metadata = Some(childMetadata))
    Await.result(derivativesCollection.insertMany(parentChildMappingOne :: parentChildMappingTwo :: Nil).toFuture(), 5.seconds)

    val metadataMap: List[MetaString] = List(MetaString("doi", "10.1101/327015"))
    val prefetchMsg: PrefetchMsg = PrefetchMsg("ingress/derivatives/remote/http/path/to/unarchived_parent.zip/child.txt", None, Some(List("a-tag")), Some(metadataMap), Some(true))
    val parentUpdate = Await.result(handler.processParent(childDoc, prefetchMsg), 5 seconds).asInstanceOf[UpdateResult]
    assert(parentUpdate.getMatchedCount == 2)
    assert(parentUpdate.getModifiedCount == 2)
    val firstMapping = Await.result(derivativesCollection.find(and(Mequal("parent", parentIdOne), Mequal("child", childId))).toFuture(), 5.seconds)
    assert(firstMapping.length == 1)
    assert(firstMapping.head.childPath == "local/derivatives/remote/http/path/to/unarchived_parent.zip/child.txt")
    assert(firstMapping.head._id == firstMappingId)
    assert(firstMapping.head.parent == parentIdOne)
    assert(firstMapping.head.child.contains(childId))
    assert(firstMapping.head.metadata.contains(childMetadata))
    val secondMapping = Await.result(derivativesCollection.find(and(Mequal("parent", parentIdTwo), Mequal("child", childId))).toFuture(), 5.seconds)
    assert(secondMapping.length == 1)
    assert(secondMapping.head.childPath == "local/derivatives/remote/http/path/to/unarchived_parent.zip/child.txt")
    assert(secondMapping.head._id == secondMappingId)
    assert(secondMapping.head.parent == parentIdTwo)
    assert(secondMapping.head.child.contains(childId))
    assert(secondMapping.head.metadata.contains(childMetadata))
  }

  "A derivative message" should "cause doc to be updated with derivative true" in {
    val createdTime = LocalDateTime.now().toInstant(ZoneOffset.UTC)
    val parentIdOne = new ObjectId()
    val parentDocOne = DoclibDoc(
      _id = parentIdOne,
      source = "ingress/derivatives/raw.txt",
      hash = "2d282102fa671256327d4767ec23bc6b",
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

    assert(parentResultOne.exists(_.wasAcknowledged()))

    val prefetchMsg: PrefetchMsg = PrefetchMsg("ingress/derivatives/raw.txt", Some(origin), Some(List("a-tag")), Some(metadataMap), Some(true))

    val docUpdate: Option[DoclibDoc] = Await.result(handler.updateDatabaseRecord(FoundDoc(parentDocOne), prefetchMsg), 5 seconds)

    docUpdate.value.derivative should be(true)
    Files.exists(Paths.get("test/prefetch-test/local/derivatives/raw.txt").toAbsolutePath) should be(true)

    docUpdate.value.uuid should not be None
  }

  "A file with a space in the path" should "be found" in {
    val docLocation = "local/test file.txt"
    val prefetchUri = handler.PrefetchUri(docLocation, None)

    val origDoc = Await.result(handler.findLocalDocument(prefetchUri), 5.seconds)
    val fetchedDoc = Await.result(handler.findLocalDocument(prefetchUri), 5.seconds)

    def docId(d: FoundDoc) = d.doc._id

    def uuid(d: FoundDoc) = d.doc.uuid

    origDoc.map(docId) should be(fetchedDoc.map(docId))
    origDoc.flatMap(uuid) should be(fetchedDoc.flatMap(uuid))

    fetchedDoc.flatMap(uuid).nonEmpty should be(true)
  }

  "A redirected url" should "be persisted in the origin" in {
    val uri = Uri.parse("https://ndownloader.figshare.com/files/3906475")
    Await.result(handler.remoteClient.resolve(uri), 5.seconds) match {
      case canonical :: rest =>
        assert(canonical.uri.get != uri)
        assert(rest.head.uri.get == uri)
      case Nil => fail("no origins found")
    }
  }

  "Multiple urls which redirect to the same url" should "be inserted in the origin as metadata" in {
    val sourceRedirect = "https://github.com/nginx/nginx/raw/master/conf/fastcgi.conf"
    val uriWithRedirect = Uri.parse(sourceRedirect)
    val similarUri = "http://github.com/nginx/nginx/raw/master/conf/fastcgi.conf"
    val similarUriUriWithRedirect = Uri.parse(similarUri)
    val canonicalUri = Uri.parse("https://raw.githubusercontent.com/nginx/nginx/master/conf/fastcgi.conf")

    // create initial document
    val firstDoc = Await.result(handler.findDocument(handler.PrefetchUri(sourceRedirect, Some(uriWithRedirect))), Duration.Inf).value
    val docLibDoc = Await.result(handler.updateDatabaseRecord(firstDoc, PrefetchMsg(uriWithRedirect.toString())), Duration.Inf).value

    docLibDoc.origin.get match {
      case canonical :: rest =>
        assert(canonical.uri.get == canonicalUri)
        assert(rest.head.uri.get == uriWithRedirect)
      case _ => fail("Expected origins to be a list")
    }

    val secondDoc = Await.result(handler.findDocument(handler.PrefetchUri(similarUri, Some(similarUriUriWithRedirect))), Duration.Inf).get
    assert(secondDoc.doc._id == firstDoc.doc._id)

    firstDoc.doc.uuid should not be None
    secondDoc.doc.uuid should be(firstDoc.doc.uuid)

    val updatedDocLibDoc = Await.result(handler.updateDatabaseRecord(secondDoc, PrefetchMsg(uriWithRedirect.toString())), Duration.Inf).get
    assert(updatedDocLibDoc.origin.get.size == 3)

    updatedDocLibDoc.uuid should be(firstDoc.doc.uuid)

    updatedDocLibDoc.origin.get match {
      case canonical :: rest =>
        assert(canonical.uri.get == canonicalUri)
        assert(rest.head.uri.get == uriWithRedirect)
        assert(rest(1).uri.get == similarUriUriWithRedirect)
      case _ => fail("Expected origins to be a list")
    }
  }


  "Adding the same url to a doc" should "not be be result in a duplicate origins" in {
    // @todo: depends on previous tests output, needs refactor to isolate with test fixture
    val source = "http://github.com/nginx/nginx/raw/master/conf/fastcgi.conf"
    val similarUri = Uri.parse(source)
    val doc = Await.result(handler.findDocument(handler.PrefetchUri(source, Some(similarUri))), Duration.Inf).get
    assert(doc.origins.size == 3)
  }

  "Adding the derivative value" should "result in a derivative document" in {
    val doc = Await.result(handler.findDocument(handler.PrefetchUri("ingress/derivative-test.txt", None), derivative = true), Duration.Inf).get
    assert(doc.doc.derivative)
  }

  "Prefetch handler" can "calculate if a file has zero length " in {
    val source = "ingress/zero_length_file.txt"
    assert(handler.zeroLength(source))
  }

  "Prefetch handler" can "calculate if a file has length greater than zero " in {
    val source = "ingress/non_zero_length_file.txt"
    assert(!handler.zeroLength(source))
  }

  "If a file has zero length it" should "not be processed" in {
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
    val result = Await.result(handler.processFoundDocument(FoundDoc(doc), "ingress/zero_length_file.txt", handler.getLocalUpdateTargetPath, handler.inLocalRoot), Duration.Inf)
    assert(result.isLeft)
    result.left.map(e => assert(e.isInstanceOf[ZeroLengthFileException]))
  }

  "Processing the same doc with additional metadata" should "add the metadata to the doclib doc" in {
    val createdTime = LocalDateTime.now().toInstant(ZoneOffset.UTC)
    val docId = new ObjectId()
    val doclibDoc = DoclibDoc(
      _id = docId,
      source = "local/metadata-tags-test/file.txt",
      hash = "2d282102fa671256327d4767ec23bc6b",
      derivative = false,
      derivatives = None,
      created = LocalDateTime.ofInstant(createdTime, ZoneOffset.UTC),
      updated = LocalDateTime.ofInstant(createdTime, ZoneOffset.UTC),
      mimetype = "text/plain",
      tags = Some(List[String]("one")),
      metadata = Some(List(MetaString("key", "value"))),
      uuid = Some(UUID.randomUUID())
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

    result.value.wasAcknowledged() should be(true)

    val prefetchMsg: PrefetchMsg = PrefetchMsg("ingress/metadata-tags-test/file.txt", Some(origin), Some(extraTags), Some(metadataMap), Some(false))
    val docUpdate: Option[DoclibDoc] = Await.result(handler.updateDatabaseRecord(FoundDoc(doclibDoc), prefetchMsg), 5 seconds)

    docUpdate.value.metadata.value should contain only (doclibDoc.metadata.getOrElse(Nil) ::: metadataMap: _*)
    docUpdate.value.tags.value should contain only (doclibDoc.tags.getOrElse(Nil) ::: extraTags: _*)

    docUpdate.value.uuid should be(doclibDoc.uuid)
  }

  "A doc with no tags or metadata" can "have new tags and metadata added" in {
    val createdTime = LocalDateTime.now().toInstant(ZoneOffset.UTC)
    val docId = new ObjectId()
    val doclibDoc = DoclibDoc(
      _id = docId,
      source = "local/metadata-tags-test/file2.txt",
      hash = "2d282102fa671256327d4767ec23bc6b",
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

    assert(result.get.wasAcknowledged())

    val prefetchMsg: PrefetchMsg = PrefetchMsg("ingress/metadata-tags-test/file2.txt", Some(origin), Some(extraTags), Some(metadataMap), Some(false))
    val docUpdate: Option[DoclibDoc] = Await.result(handler.updateDatabaseRecord(FoundDoc(doclibDoc), prefetchMsg), 5 seconds)

    docUpdate.value.metadata.value should contain only (metadataMap: _*)
    docUpdate.value.tags.value should contain only (extraTags: _*)

    docUpdate.value should not be None
  }

  "Different zero length files" should "have the same md5" in {
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
      fileHash should (equal(fileHash2) and equal(fileHash3))
    }
  }

  "A file with the same contents" should "not create two docs" in {
    val docLocation = "ingress/zero_length_file.txt"
    val prefetchUri = handler.PrefetchUri(docLocation, None)
    val docLocation2 = "ingress/zero_length_file2.txt"
    val prefetchUri2 = handler.PrefetchUri(docLocation2, None)
    val origDoc = Await.result(handler.findLocalDocument(prefetchUri), 5.seconds).get
    val fetchedDoc = Await.result(handler.findLocalDocument(prefetchUri2), 5.seconds).get
    assert(origDoc.doc._id == fetchedDoc.doc._id)
  }

  "A file with the same name but different contents" should "create two docs" in {
    val doclibRoot = config.getString("doclib.root")
    val ingressDir = config.getString("doclib.local.temp-dir")
    val firstDoc = "aFile.txt"
    val secondDoc = "aFile.txt"
    val docFile: ScalaFile = ScalaFile(s"$doclibRoot/$ingressDir/first/$firstDoc").createFileIfNotExists(createParents = true)
    val docFile2: ScalaFile = ScalaFile(s"$doclibRoot/$ingressDir/second/$secondDoc").createFileIfNotExists(createParents = true)
    docFile.appendLine("Some contents")
    docFile2.appendLine("Different contents")
    val origDoc = Await.result(handler.findLocalDocument(handler.PrefetchUri(Paths.get(ingressDir, "first", firstDoc).toString, None)), 5.seconds).get
    val fetchedDoc = Await.result(handler.findLocalDocument(handler.PrefetchUri(Paths.get(ingressDir, "second", firstDoc).toString, None)), 5.seconds).get
    assert(origDoc.doc._id != fetchedDoc.doc._id)
  }

  "Processing a parent with existing child" should "update parent-child mapping" in {
    val createdTime = LocalDateTime.now().toInstant(ZoneOffset.UTC)
    val childMetadata: List[MetaValueUntyped] = List[MetaValueUntyped](MetaString("metadata-key", "metadata-value"))
    val parentIdOne = new ObjectId()
    val childId = new ObjectId()
    val parentDocOne = DoclibDoc(
      _id = parentIdOne,
      source = "remote/http/path/to/parent.zip",
      hash = "12345",
      derivative = false,
      created = LocalDateTime.ofInstant(createdTime, ZoneOffset.UTC),
      updated = LocalDateTime.ofInstant(createdTime, ZoneOffset.UTC),
      mimetype = "text/plain",
      tags = Some(List[String]())
    )
    val parentChildMapping = ParentChildMapping(_id = UUID.randomUUID(), parent = parentIdOne, childPath = "ingress/derivatives/remote/http/path/to/unarchived_parent.zip/child.txt", metadata = Some(childMetadata), consumer = Some("unarchived"))
    val parentChildInsert = Await.result(derivativesCollection.insertOne(parentChildMapping).toFutureOption(), 5.seconds)
    assert(parentChildInsert.exists(_.wasAcknowledged()))
    val parentResultOne = Await.result(collection.insertOne(parentDocOne).toFutureOption(), 5 seconds)

    assert(parentResultOne.exists(_.wasAcknowledged()))

    val childDoc = DoclibDoc(
      _id = childId,
      source = "local/derivatives/remote/http/path/to/unarchived_parent.zip/child.txt",
      hash = "12345",
      derivative = true,
      created = LocalDateTime.ofInstant(createdTime, ZoneOffset.UTC),
      updated = LocalDateTime.ofInstant(createdTime, ZoneOffset.UTC),
      mimetype = "text/plain",
      tags = Some(List[String]()),
    )
    val childResult = collection.insertOne(childDoc).toFutureOption()
    whenReady(childResult, Timeout(Span(20, Seconds))) { result =>
      assert(result.exists(_.wasAcknowledged()))
    }
    val metadataMap: List[MetaString] = List(MetaString("doi", "10.1101/327015"))
    val prefetchMsg: PrefetchMsg = PrefetchMsg("ingress/derivatives/remote/http/path/to/unarchived_parent.zip/child.txt", None, Some(List("a-tag")), Some(metadataMap), Some(true))
    val parentUpdate = handler.processParent(childDoc, prefetchMsg)
    whenReady(parentUpdate, Timeout(Span(20, Seconds))) { result =>
      val updateResult = result.asInstanceOf[UpdateResult]
      assert(updateResult.wasAcknowledged())
      assert(updateResult.getMatchedCount == 1)
      assert(updateResult.getModifiedCount == 1)
    }
    val firstMapping: Future[Seq[ParentChildMapping]] = derivativesCollection.find(and(Mequal("parent", parentIdOne), Mequal("child", childId))).toFuture()
    whenReady(firstMapping, Timeout(Span(20, Seconds))) { result: Seq[ParentChildMapping] =>
      assert(result.length == 1)
      assert(result.head.childPath == "local/derivatives/remote/http/path/to/unarchived_parent.zip/child.txt")
      assert(result.head.parent == parentIdOne)
      assert(result.head.child.contains(childId))
      assert(result.head.metadata.contains(childMetadata))
      assert(result.head.consumer.contains("unarchived"))
    }

  }

  "Processing a derivative with no parent-child mapping" should "not update or create any parent-child mappings" in {
    val createdTime = LocalDateTime.now().toInstant(ZoneOffset.UTC)
    val childId = new ObjectId()

    val childDoc = DoclibDoc(
      _id = childId,
      source = "local/derivatives/remote/http/path/to/unarchived_parent.zip/child.txt",
      hash = "12345",
      derivative = true,
      created = LocalDateTime.ofInstant(createdTime, ZoneOffset.UTC),
      updated = LocalDateTime.ofInstant(createdTime, ZoneOffset.UTC),
      mimetype = "text/plain",
      tags = Some(List[String]()),
    )
    val childResult = collection.insertOne(childDoc).toFutureOption()
    whenReady(childResult, Timeout(Span(20, Seconds))) { result =>
      assert(result.exists(_.wasAcknowledged()))
    }
    val metadataMap: List[MetaString] = List(MetaString("doi", "10.1101/327015"))
    val prefetchMsg: PrefetchMsg = PrefetchMsg("ingress/derivatives/remote/http/path/to/unarchived_parent.zip/child.txt", None, Some(List("a-tag")), Some(metadataMap), Some(true))
    val parentUpdate = handler.processParent(childDoc, prefetchMsg)
    whenReady(parentUpdate, Timeout(Span(20, Seconds))) { result =>
      val updateResult = result.asInstanceOf[UpdateResult]
      assert(updateResult.wasAcknowledged())
      assert(updateResult.getMatchedCount == 0)
      assert(updateResult.getModifiedCount == 0)
    }
  }

  override def beforeAll(): Unit = {
    Await.result(collection.drop().toFuture(), 5.seconds)
    Await.result(derivativesCollection.drop().toFuture(), 5.seconds)
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
      Files.copy(Paths.get("test/raw.txt").toAbsolutePath, Paths.get("test/prefetch-test/ingress/derivative-test.txt").toAbsolutePath)
    }
  }
}
