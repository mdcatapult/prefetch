package io.mdcatapult.doclib.handlers

import akka.actor._
import akka.testkit.{ImplicitSender, TestKit}
import com.typesafe.config.ConfigFactory
import io.mdcatapult.doclib.messages.PrefetchMsg
import io.mdcatapult.doclib.models.DoclibDoc
import io.mdcatapult.doclib.prefetch.model.Exceptions.SilentValidationException
import org.bson.types.ObjectId
import org.scalamock.scalatest.MockFactory
import org.scalatest.BeforeAndAfterEach
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

import java.io.FileNotFoundException
import java.nio.file.{Files, Paths}
import java.time.LocalDateTime
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps
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
  with PrefetchHandlerBaseTest {

  import system.dispatcher

  private val handler = new PrefetchHandler(downstream, archiver, readLimiter, writeLimiter)
  private val ingressFilenameWithPath = "ingress/test_1.csv"
  private val awaitDuration = 5 seconds

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

      val futureResult = for {
        _ <- collection.insertOne(doclibDoc).toFuture()
        inputMessage = PrefetchMsg(ingressFilenameWithPath, verify = Option(true))
        handlerRes <- handler.handle(inputMessage)
      } yield handlerRes

      intercept[SilentValidationException] {
        Await.result(futureResult, awaitDuration)
      }

  }

    it should "return a FileNotFoundException given an incorrect file path" in {
      val nonExistentFile = "bingress/blah.csv"
      val inputMessage = PrefetchMsg(nonExistentFile, verify = Option(true))

      intercept[FileNotFoundException] {
        Await.result(handler.handle(inputMessage), awaitDuration)
      }
    }

    it should "return an instance of NewAndFoundDoc given a valid message and file exists in the ingress path" in {
      val inputMessage = PrefetchMsg(ingressFilenameWithPath)
      val result = Await.result(handler.handle(inputMessage), awaitDuration).get
      assert(result.foundDoc.doc.source == "ingress/test_1.csv")
    }

  override def beforeEach(): Unit = {
    Await.result(collection.drop().toFuture(), 5 seconds)
    Await.result(derivativesCollection.drop().toFuture(), 5 seconds)

    Try {
      Files.createDirectories(Paths.get("test/prefetch-test/ingress/derivatives").toAbsolutePath)
      Files.createDirectories(Paths.get("test/prefetch-test/local").toAbsolutePath)
      Files.copy(Paths.get("test/test_1.csv").toAbsolutePath, Paths.get("test/prefetch-test/ingress/test_1.csv").toAbsolutePath)
    }
  }
}
