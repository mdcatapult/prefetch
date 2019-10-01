package io.mdcatapult.doclib.handlers

import java.util.Date

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.testkit.{ImplicitSender, TestKit}
import better.files.Dsl._
import better.files.{File ⇒ ScalaFile}
import com.mongodb.async.client.{MongoCollection ⇒ JMongoCollection}
import com.typesafe.config.{Config, ConfigFactory}
import io.mdcatapult.doclib.messages.{DoclibMsg, PrefetchMsg}
import io.mdcatapult.doclib.util.{FileHash, MongoCodecs}
import io.mdcatapult.klein.queue.Sendable
import org.bson.codecs.configuration.CodecRegistry
import org.mongodb.scala.bson.{BsonDateTime, BsonDocument, BsonDouble, BsonNull, BsonString}
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
  with BeforeAndAfterAll with MockFactory with FileHash with OptionValues {

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
  val wrappedCollection: JMongoCollection[Document] = stub[JMongoCollection[Document]]
  implicit val collection: MongoCollection[Document] = MongoCollection[Document](wrappedCollection)

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
        val document = Document(
          BsonDocument(
            "key" → BsonString("unarchived"),
            "source" → BsonString(tempFile.path.toString.stripPrefix("/")),
            "version" → BsonDouble(2.0),
            "hash" → BsonString(fileHash),
            "started" → BsonDateTime(new Date()),
            "ended" → BsonNull())
        )
        val foundDoc = new handler.FoundDoc(document, None, None, None)
        val actualMovedFilePath = handler.handleFileUpdate(foundDoc, tempFile.path.toString, handler.getLocalUpdateTargetPath, handler.inLocalRoot)
        val movedFilePath = ScalaFile(s"${testBase}/local/${docFile.pathAsString}")
        assert(actualMovedFilePath.value.toString == movedFilePath.pathAsString)
      }
    }
  }

  override def afterAll(): Unit = {
    Seq((pwd/"test/local"),
      (pwd/"test"/"efs"),
      (pwd/"test"/"ftp"),
      (pwd/"test"/"http"),
      (pwd/"test"/"https"),
      (pwd/"test"/"remote-ingress"),
      (pwd/"test"/"ingress"),
      (pwd/"test"/"local")).map(_.delete(true))
  }
}
