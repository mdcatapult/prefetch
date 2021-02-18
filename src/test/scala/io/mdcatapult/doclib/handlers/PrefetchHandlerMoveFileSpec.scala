package io.mdcatapult.doclib.handlers

import java.time.{LocalDateTime, ZoneOffset}

import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.testkit.{ImplicitSender, TestKit}
import better.files.Dsl._
import better.files.File.Attributes
import better.files.{File => ScalaFile}
import com.mongodb.reactivestreams.client.{MongoCollection => JMongoCollection}
import com.typesafe.config.{Config, ConfigFactory}
import io.mdcatapult.doclib.codec.MongoCodecs
import io.mdcatapult.doclib.messages.{DoclibMsg, PrefetchMsg}
import io.mdcatapult.doclib.models.{DoclibDoc, FileAttrs, ParentChildMapping}
import io.mdcatapult.doclib.remote.DownloadResult
import io.mdcatapult.klein.queue.Sendable
import io.mdcatapult.util.concurrency.SemaphoreLimitedExecution
import io.mdcatapult.util.hash.Md5.md5
import io.mdcatapult.util.path.DirectoryDeleter.deleteDirectories
import org.bson.codecs.configuration.CodecRegistry
import org.mongodb.scala.MongoCollection
import org.mongodb.scala.bson.ObjectId
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import org.scalatest.{BeforeAndAfterAll, OptionValues}

import scala.concurrent.ExecutionContext.Implicits.global

/**
 * Test prefetch handler moving files around from source to target
 */
class PrefetchHandlerMoveFileSpec extends TestKit(ActorSystem("PrefetchHandlerSpec", ConfigFactory.parseString("""
  akka.loggers = ["akka.testkit.TestEventListener"]
  """))) with ImplicitSender
  with AnyWordSpecLike
  with Matchers
  with BeforeAndAfterAll with MockFactory with OptionValues {

  implicit val config: Config = ConfigFactory.parseString(
    s"""
       |consumer {
       |  name = "prefetch"
       |}
       |appName = $${?consumer.name}
       |doclib {
       |  root: "${pwd/"test"}"
       |  flag: "prefetch"
       |  local {
       |    target-dir: "local"
       |    temp-dir: "ingress"
       |  }
       |  remote {
       |    target-dir: "remote"
       |    temp-dir: "remote-ingress"
       |  }
       |  archive {
       |    target-dir: "archive"
       |  }
       |  derivative {
       |    target-dir: "derivatives"
       |  }
       |}
       |version {
       |  number = "1.2.3",
       |  major = 1,
       |  minor = 2,
       |  patch = 3,
       |  hash =  "12345"
       |}
    """.stripMargin)

  implicit val m: Materializer = Materializer(system)

  implicit val mongoCodecs: CodecRegistry = MongoCodecs.get
  val wrappedCollection: JMongoCollection[DoclibDoc] = stub[JMongoCollection[DoclibDoc]]
  val wrappedPCCollection: JMongoCollection[ParentChildMapping] = stub[JMongoCollection[ParentChildMapping]]
  implicit val collection: MongoCollection[DoclibDoc] = MongoCollection[DoclibDoc](wrappedCollection)
  implicit val derivativesCollection: MongoCollection[ParentChildMapping] = MongoCollection[ParentChildMapping](wrappedPCCollection)

  implicit val upstream: Sendable[PrefetchMsg] = stub[Sendable[PrefetchMsg]]
  val downstream: Sendable[DoclibMsg] = stub[Sendable[DoclibMsg]]
  val archiver: Sendable[DoclibMsg] = stub[Sendable[DoclibMsg]]

  private val readLimiter = SemaphoreLimitedExecution.create(1)
  private val writeLimiter = SemaphoreLimitedExecution.create(1)

  val handler = new PrefetchHandler(downstream, archiver, readLimiter, writeLimiter)

  "The handler" should {

    "move a new local file to the correct directory" in {
      implicit val attributes: Attributes = ScalaFile.Attributes.default
      val sourceFile = "aFile.txt"
      val doclibRoot = config.getString("doclib.root")
      val ingressDir = config.getString("doclib.local.temp-dir")
      val localTargetDir = config.getString("doclib.local.target-dir")
      val docFile: ScalaFile = ScalaFile(s"$doclibRoot/$ingressDir/$sourceFile").createFileIfNotExists(createParents = true)
      docFile.appendLine("Not an empty file")
      for {
        tempFile <- docFile.toTemporary
      } {
        val fileHash = md5(tempFile.toJava)
        val createdTime = LocalDateTime.now().toInstant(ZoneOffset.UTC)
        val fileAttrs = FileAttrs(
          path = tempFile.path.getParent.toAbsolutePath.toString,
          name = tempFile.path.getFileName.toString,
          mtime = LocalDateTime.ofInstant(createdTime, ZoneOffset.UTC),
          ctime = LocalDateTime.ofInstant(createdTime, ZoneOffset.UTC),
          atime = LocalDateTime.ofInstant(createdTime, ZoneOffset.UTC),
          size = 5
        )
        val document = DoclibDoc(
          _id = new ObjectId(),
          source = tempFile.pathAsString,
          hash = fileHash,
          derivative = false,
          created = LocalDateTime.ofInstant(createdTime, ZoneOffset.UTC),
          updated = LocalDateTime.ofInstant(createdTime, ZoneOffset.UTC),
          mimetype = "text/plain",
          attrs = Some(fileAttrs)
        )
        val foundDoc = FoundDoc(document, Nil, Nil, None)
        val actualMovedFilePath = handler.archiveOrProcess(foundDoc, s"$ingressDir/$sourceFile", handler.getLocalUpdateTargetPath, handler.inLocalRoot)
        val movedFilePath = s"$doclibRoot/$localTargetDir/$sourceFile"
        assert(actualMovedFilePath.get.toString == movedFilePath)
      }
    }

    "move a new remote https file to the correct directory" in {
      implicit val attributes: Attributes = ScalaFile.Attributes.default
      val sourceFile = "https/path/to/aFile.txt"
      val doclibRoot = config.getString("doclib.root")
      val remoteIngressDir = config.getString("doclib.remote.temp-dir")
      val remoteTargetDir = config.getString("doclib.remote.target-dir")
      val docFile: ScalaFile = ScalaFile(s"$doclibRoot/$remoteIngressDir/$sourceFile").createFileIfNotExists(createParents = true)
      docFile.appendLine("Not an empty file")
      for {
        tempFile <- docFile.toTemporary
      } {
        val fileHash = md5(tempFile.toJava)
        val createdTime = LocalDateTime.now().toInstant(ZoneOffset.UTC)
        val fileAttrs = FileAttrs(
          path = tempFile.path.getParent.toAbsolutePath.toString,
          name = tempFile.path.getFileName.toString,
          mtime = LocalDateTime.ofInstant(createdTime, ZoneOffset.UTC),
          ctime = LocalDateTime.ofInstant(createdTime, ZoneOffset.UTC),
          atime = LocalDateTime.ofInstant(createdTime, ZoneOffset.UTC),
          size = 5
        )
        val document = DoclibDoc(
          _id = new ObjectId(),
          source = "https://path/to/aFile.txt",
          hash = fileHash,
          derivative = false,
          created = LocalDateTime.ofInstant(createdTime, ZoneOffset.UTC),
          updated = LocalDateTime.ofInstant(createdTime, ZoneOffset.UTC),
          mimetype = "text/html",
          attrs = Some(fileAttrs)
        )
        val foundDoc = FoundDoc(document, Nil, Nil, Some(DownloadResult(docFile.pathAsString, fileHash, Some("https://path/to/aFile.txt"), Some(s"${config.getString("doclib.remote.target-dir")}/https/path/to/aFile.txt"))))
        val actualMovedFilePath = handler.archiveOrProcess(foundDoc, s"$remoteIngressDir/$sourceFile", handler.getRemoteUpdateTargetPath, handler.inRemoteRoot)
        val movedFilePath = s"$doclibRoot/$remoteTargetDir/https/path/to/aFile.txt"
        assert(actualMovedFilePath.get.toString == movedFilePath)
      }
    }

    "not archive an existing derivative" in {
      implicit val attributes: Attributes = ScalaFile.Attributes.default
      val sourceFile = "https/path/to/aFile.txt"
      val doclibRoot = config.getString("doclib.root")
      val remoteIngressDir = config.getString("doclib.remote.temp-dir")
      val remoteTargetDir = config.getString("doclib.remote.target-dir")
      val docFile: ScalaFile = ScalaFile(s"$doclibRoot/$remoteTargetDir/$sourceFile").createFileIfNotExists(createParents = true)
      val newFile: ScalaFile = ScalaFile(s"$doclibRoot/$remoteIngressDir/$sourceFile").createFileIfNotExists(createParents = true)
      docFile.appendLine("Not an empty file")
      newFile.appendLine("Also not an empty file")
      for {
        tempDocFile <- docFile.toTemporary
      } {
        val docFileHash = md5(tempDocFile.toJava)
        val createdTime = LocalDateTime.now().toInstant(ZoneOffset.UTC)
        val fileAttrs = FileAttrs(
          path = tempDocFile.path.getParent.toAbsolutePath.toString,
          name = tempDocFile.path.getFileName.toString,
          mtime = LocalDateTime.ofInstant(createdTime, ZoneOffset.UTC),
          ctime = LocalDateTime.ofInstant(createdTime, ZoneOffset.UTC),
          atime = LocalDateTime.ofInstant(createdTime, ZoneOffset.UTC),
          size = 5
        )
        val document = DoclibDoc(
          _id = new ObjectId(),
          source = "https://path/to/aFile.txt",
          hash = docFileHash,
          derivative = true,
          created = LocalDateTime.ofInstant(createdTime, ZoneOffset.UTC),
          updated = LocalDateTime.ofInstant(createdTime, ZoneOffset.UTC),
          mimetype = "text/html",
          attrs = Some(fileAttrs)
        )
        val foundDoc = FoundDoc(document, Nil, Nil, Some(DownloadResult(docFile.pathAsString, docFileHash, Some("https://path/to/aFile.txt"), Some(s"${config.getString("doclib.remote.target-dir")}/https/path/to/aFile.txt"))))
        val actualMovedFilePath = handler.archiveOrProcess(foundDoc, s"$remoteIngressDir/$sourceFile", handler.getRemoteUpdateTargetPath, handler.inRemoteRoot)
        val movedFilePath = s"$doclibRoot/$remoteTargetDir/https/path/to/aFile.txt"
        assert(actualMovedFilePath.get.toString == movedFilePath)
      }
    }
  }

  override def afterAll(): Unit = {
    // These may or may not exist but are all removed anyway

    deleteDirectories(Seq(pwd/"test/local",
      pwd/"test"/"efs",
      pwd/"test"/"ftp",
      pwd/"test"/"http",
      pwd/"test"/"https",
      pwd/"test"/"remote-ingress",
      pwd/"test"/"ingress",
      pwd/"test"/"remote"))
  }

}
