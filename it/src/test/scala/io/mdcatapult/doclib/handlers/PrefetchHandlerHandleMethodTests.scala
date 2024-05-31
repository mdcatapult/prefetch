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

import com.typesafe.config.ConfigFactory
import io.mdcatapult.doclib.messages.{DoclibMsg, PrefetchMsg, SupervisorMsg}
import io.mdcatapult.doclib.models.DoclibDoc
import io.mdcatapult.doclib.prefetch.model.Exceptions.SilentValidationException
import io.mdcatapult.klein.queue.Sendable
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.testkit.{ImplicitSender, TestKit}
import org.bson.types.ObjectId
import org.scalamock.scalatest.MockFactory
import org.scalatest.BeforeAndAfterEach
import org.scalatest.RecoverMethods.recoverToSucceededIf
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.SpanSugar

import java.io.FileNotFoundException
import java.nio.file.{Files, Paths}
import java.time.LocalDateTime
import scala.concurrent.{Await, Future}
import scala.util.Try

class PrefetchHandlerHandleMethodTests extends TestKit(ActorSystem("PrefetchHandlerHandleMethodTest", ConfigFactory.parseString(
  """
  akka.loggers = ["akka.testkit.TestEventListener"]
  """))) with ImplicitSender
  with AnyFlatSpecLike
  with Matchers
  with BeforeAndAfterEach
  with MockFactory
  with ScalaFutures
  with PrefetchHandlerBaseTest
  with SpanSugar {

  import system.dispatcher

  implicit val upstream: Sendable[PrefetchMsg] = stub[Sendable[PrefetchMsg]]
  val downstream: Sendable[SupervisorMsg] = stub[Sendable[SupervisorMsg]]
  val archiver: Sendable[DoclibMsg] = stub[Sendable[DoclibMsg]]

  private val handler = new PrefetchHandler(downstream, readLimiter, writeLimiter)
  private val ingressFilenameWithPath = "ingress/test_1.csv"
  private val ingressFilenameWithPath2 = "ingress/test_2.txt"
  private val awaitDuration = 5.seconds

  "The PrefetchHandler handle method" should
    "return a SilentValidationException given a db record exists from the previous day" in {
      val currentTimeMinusOneDay = LocalDateTime.now().minusDays(1L)
      val doclibDoc = DoclibDoc(
        _id = new ObjectId("5fce14191ba6254dea8dcb83"),
        source = ingressFilenameWithPath,
        hash = "7fb875d2de06a19591efbd6327be4685",
        mimetype = "",
        created = currentTimeMinusOneDay,
        updated = currentTimeMinusOneDay,
      )
      val prefetchMsg = PrefetchMsg(source = ingressFilenameWithPath, verify = Some(true))

    recoverToSucceededIf[SilentValidationException] {
      for {
        _ <- collection.insertOne(doclibDoc).toFuture()
        handlerRes <- handler.handle(PrefetchMsgCommittableReadResult(prefetchMsg))
      } yield handlerRes
    }
  }

    it should "return a FileNotFoundException given an incorrect file path" in {
      val nonExistentFile = "bingress/blah.csv"
      val inputMessage = PrefetchMsg(nonExistentFile, verify = Option(true))

      whenReady(handler.handle(PrefetchMsgCommittableReadResult(inputMessage)), timeout(awaitDuration)) { result =>
        assert(result._2.isFailure)
        assert(result._2.failed.get.isInstanceOf[FileNotFoundException])
      }

    }

    it should "return an instance of NewAndFoundDoc given a valid message and file exists in the ingress path" in {
      val inputMessage = PrefetchMsg(ingressFilenameWithPath2)
      val futureResult = handler.handle(PrefetchMsgCommittableReadResult(inputMessage))
      whenReady(futureResult, timeout(awaitDuration)) { result =>
        assert(result._2.get.foundDoc.doc.source == "ingress/test_2.txt")
      }
    }

  it should "find a doc in the final result" in {
    val doclibDoc = DoclibDoc(
      _id = new ObjectId("5fce14191ba6254dea8dcb83"),
      source = "blah",
      hash = "7fb875d2de06a19591efbd6327be4685",
      mimetype = "",
      created = LocalDateTime.now(),
      updated = LocalDateTime.now()
    )
    val foundDoc = FoundDoc(doc = doclibDoc)
    val prefetchResult = PrefetchResult(doclibDoc = doclibDoc,foundDoc = foundDoc)
    val prefetchMsg = PrefetchMsg("blah")
    val committableReadResult = PrefetchMsgCommittableReadResult(prefetchMsg)
    whenReady(handler.finalResult(Future.successful(Right(prefetchResult)), committableReadResult, foundDoc), timeout(awaitDuration)) { result =>
      assert(result._2.get.foundDoc.doc.source == "blah")
    }
  }

  override def beforeEach(): Unit = {
    Await.result(collection.drop().toFuture(), awaitDuration)
    Await.result(derivativesCollection.drop().toFuture(), awaitDuration)

    Try {
      Files.createDirectories(Paths.get("test/prefetch-test/ingress/derivatives").toAbsolutePath)
      Files.createDirectories(Paths.get("test/prefetch-test/local").toAbsolutePath)
      Files.copy(Paths.get("test/test_1.csv").toAbsolutePath, Paths.get("test/prefetch-test/ingress/test_1.csv").toAbsolutePath)
      Files.copy(Paths.get("test/raw.txt").toAbsolutePath, Paths.get("test/prefetch-test/ingress/test_2.txt").toAbsolutePath)
    }
  }
}
