package io.mdcatapult.doclib.handlers

import java.time.{LocalDateTime, ZoneOffset}
import java.util.Date

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.testkit.{ImplicitSender, TestKit}
import better.files.Dsl._
import better.files.{File ⇒ ScalaFile}
import com.mongodb.async.client.{MongoCollection ⇒ JMongoCollection}
import com.typesafe.config.{Config, ConfigFactory}
import io.mdcatapult.doclib.TestDirectoryDelete
import io.mdcatapult.doclib.messages.{DoclibMsg, PrefetchMsg}
import io.mdcatapult.doclib.models.{DoclibDoc, FileAttrs}
import io.mdcatapult.doclib.remote.DownloadResult
import io.mdcatapult.doclib.util.{FileHash, MongoCodecs}
import io.mdcatapult.klein.queue.Sendable
import org.bson.codecs.configuration.CodecRegistry
import org.mongodb.scala.bson.{BsonDateTime, BsonDocument, BsonDouble, BsonNull, BsonString, ObjectId}
import org.mongodb.scala.{Document, MongoCollection}
import org.scalamock.scalatest.MockFactory
import org.scalatest.{BeforeAndAfterAll, Matchers, OptionValues, WordSpecLike}

import scala.concurrent.ExecutionContextExecutor

/**
 * Test prefetch handler moving files around from source to target
 */
class PrefetchHandlerMoveFileSpec extends TestKit(ActorSystem("PrefetchHandlerSpec", ConfigFactory.parseString("""
  akka.loggers = ["akka.testkit.TestEventListener"]
  """))) with ImplicitSender
  with WordSpecLike
  with Matchers
  with BeforeAndAfterAll with MockFactory with FileHash with OptionValues with TestDirectoryDelete {

  val testBase = s"$pwd/test"
  implicit val config: Config = ConfigFactory.parseString(
    s"""
       |doclib {
       |  root: "/"
       |  local {
       |    target-dir: "$testBase/local"
       |    temp-dir: "$testBase/ingress"
       |  }
       |  remote {
       |    target-dir: "$testBase/remote"
       |    temp-dir: "$testBase/remote-ingress"
       |  }
       |  archive {
       |    target-dir: "$testBase/archive"
       |  }
       |}
    """.stripMargin)

  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val executor: ExecutionContextExecutor = scala.concurrent.ExecutionContext.global
  implicit val mongoCodecs: CodecRegistry = MongoCodecs.get
  val wrappedCollection: JMongoCollection[DoclibDoc] = stub[JMongoCollection[DoclibDoc]]
  implicit val collection: MongoCollection[DoclibDoc] = MongoCollection[DoclibDoc](wrappedCollection)

  implicit val upstream: Sendable[PrefetchMsg] = stub[Sendable[PrefetchMsg]]
  val downstream: Sendable[DoclibMsg] = stub[Sendable[DoclibMsg]]
  val archiver: Sendable[DoclibMsg] = stub[Sendable[DoclibMsg]]
  val handler = new PrefetchHandler(downstream, archiver)


  "The handler" should {

    "move a new local file to the correct directory" in {
      implicit val attributes = ScalaFile.Attributes.default
      println(pwd)
      val docFile: ScalaFile = ScalaFile(s"$pwd/test/efs/aFile.txt").createFileIfNotExists(true)
      for {
        tempFile <- docFile.toTemporary
      } {
        val fileHash = md5(tempFile.path.toString)
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
          attrs = fileAttrs
        )
        val foundDoc = new handler.FoundDoc(document, None, None, None)
        val actualMovedFilePath = handler.handleFileUpdate(foundDoc, tempFile.path.toString, handler.getLocalUpdateTargetPath, handler.inLocalRoot)
        val movedFilePath = ScalaFile(s"${testBase}/local/${docFile.pathAsString}")
        assert(actualMovedFilePath.value.toString == movedFilePath.pathAsString)
      }
    }

    "move a new remote https file to the correct directory" in {
      implicit val attributes = ScalaFile.Attributes.default
      val docFile: ScalaFile = ScalaFile(s"${config.getString("doclib.remote.temp-dir")}/https/path/to/aFile.txt").createFileIfNotExists(true)
      for {
        tempFile <- docFile.toTemporary
      } {
        val fileHash = md5(tempFile.path.toString)
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
          attrs = fileAttrs
        )
        val foundDoc = new handler.FoundDoc(document, None, None, Some(DownloadResult(docFile.pathAsString, fileHash, Some("https://path/to/aFile.txt"), Some(s"${config.getString("doclib.remote.target-dir")}/https/path/to/aFile.txt"))))
        val actualMovedFilePath = handler.handleFileUpdate(foundDoc, tempFile.path.toString, handler.getRemoteUpdateTargetPath, handler.inRemoteRoot)
        val movedFilePath = ScalaFile(s"${config.getString("doclib.remote.target-dir")}/https/path/to/aFile.txt")
        assert(actualMovedFilePath.value.toString == movedFilePath.pathAsString)
      }
    }
  }

  override def afterAll(): Unit = {
    // These may or may not exist but are all removed anyway
    deleteDirectories(List((pwd/"test/local"),
      (pwd/"test"/"efs"),
      (pwd/"test"/"ftp"),
      (pwd/"test"/"http"),
      (pwd/"test"/"https"),
      (pwd/"test"/"remote-ingress"),
      (pwd/"test"/"ingress"),
      (pwd/"test"/"remote")))
  }
}
