/*
 * Copyright 2024 Medicines Discovery Catapult
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.mdcatapult.doclib.handlers

import java.time.{LocalDateTime, ZoneOffset}
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.Materializer
import org.apache.pekko.testkit.{ImplicitSender, TestKit}
import better.files.Dsl._
import better.files.File.Attributes
import better.files.{File => ScalaFile}
import com.mongodb.reactivestreams.client.{MongoCollection => JMongoCollection}
import com.typesafe.config.{Config, ConfigFactory}
import io.mdcatapult.doclib.codec.MongoCodecs
import io.mdcatapult.doclib.messages.{DoclibMsg, PrefetchMsg, SupervisorMsg}
import io.mdcatapult.doclib.models.{AppConfig, DoclibDoc, FileAttrs, ParentChildMapping}
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

import scala.concurrent.Await
import scala.concurrent.duration._
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
       |  concurrency = 20
       |  queue = "prefetch"
       |  exchange = "doclib"
       |
       |}
       |appName = $${?consumer.name}
       |doclib {
       |  root: "${pwd / "test"}"
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
  val downstream: Sendable[SupervisorMsg] = stub[Sendable[SupervisorMsg]]
  val archiver: Sendable[DoclibMsg] = stub[Sendable[DoclibMsg]]

  private val readLimiter = SemaphoreLimitedExecution.create(1)
  private val writeLimiter = SemaphoreLimitedExecution.create(1)

  implicit val appConfig: AppConfig =
    AppConfig(
      config.getString("consumer.name"),
      config.getInt("consumer.concurrency"),
      config.getString("consumer.queue"),
      Option(config.getString("consumer.exchange"))
    )


  val handler = new PrefetchHandler(downstream, readLimiter, writeLimiter)

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
        val actualMovedFilePath = Await.result(handler.ingressDocument(foundDoc, s"$ingressDir/$sourceFile", handler.getLocalUpdateTargetPath(foundDoc), handler.inLocalRoot(foundDoc.doc.source)), 5.seconds)
        val movedFilePath = s"$localTargetDir/$sourceFile"
        assert(actualMovedFilePath.isRight)
        actualMovedFilePath.map(a => assert(a.get.toString == movedFilePath))
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
        val actualMovedFilePath = Await.result(handler.ingressDocument(foundDoc, s"$remoteIngressDir/$sourceFile", handler.getRemoteUpdateTargetPath(foundDoc), handler.inRemoteRoot(foundDoc.doc.source)), 5.seconds)
        val movedFilePath = s"$remoteTargetDir/https/path/to/aFile.txt"
        assert(actualMovedFilePath.isRight)
        actualMovedFilePath.map(a => assert(a.get.toString == movedFilePath))
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
        val actualMovedFilePath = Await.result(handler.ingressDocument(foundDoc, s"$remoteIngressDir/$sourceFile", handler.getRemoteUpdateTargetPath(foundDoc), handler.inRemoteRoot(foundDoc.doc.source)), 5.seconds)
        val movedFilePath = s"$remoteTargetDir/https/path/to/aFile.txt"
        assert(actualMovedFilePath.isRight)
        actualMovedFilePath.map(a => assert(a.get.toString == movedFilePath))
      }
    }
  }

    override def afterAll(): Unit = {
      // These may or may not exist but are all removed anyway

      deleteDirectories(Seq(pwd / "test/local",
        pwd / "test" / "efs",
        pwd / "test" / "ftp",
        pwd / "test" / "http",
        pwd / "test" / "https",
        pwd / "test" / "remote-ingress",
        pwd / "test" / "ingress",
        pwd / "test" / "remote"))
    }
  }
