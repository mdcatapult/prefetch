package io.mdcatapult.doclib.handlers

import akka.actor._
import akka.testkit.{ImplicitSender, TestKit}
import com.typesafe.config.ConfigFactory
import io.mdcatapult.doclib.messages.PrefetchMsg
import io.mdcatapult.doclib.models.DoclibDoc
import org.bson.types.ObjectId
import org.scalamock.scalatest.MockFactory
import org.scalatest.BeforeAndAfterEach
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.funspec.AnyFunSpecLike
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
  with AnyFunSpecLike
  with Matchers
  with BeforeAndAfterEach
  with MockFactory
  with ScalaFutures
  with PrefetchHandlerBaseTest {

  import system.dispatcher

  private val handler = new PrefetchHandler(downstream, archiver, readLimiter, writeLimiter)
  private val ingressFilenameWithPath = "ingress/test_1.csv"
  private val awaitDuration = 5 seconds
  private val prefetchKey = "prefetch"

  describe("The PrefetchHandler handle method") {
    it("should return a SilentValidationException given a db record exists from the previous day") {
      val currentTimeMinusOneDay = LocalDateTime.now().minusDays(1L)
      val doclibDoc = DoclibDoc(
        _id = new ObjectId("5fce14191ba6254dea8dcb83"),
        source = ingressFilenameWithPath,
        hash = "064b019e7d5cfbefd87a0ef2a41951cc",
        mimetype = "",
        created = currentTimeMinusOneDay,
        updated = currentTimeMinusOneDay,
      )

      val futureResult = for {
        _ <- collection.insertOne(doclibDoc).toFuture()
        inputMessage = PrefetchMsg(ingressFilenameWithPath, verify = Option(true))
        handlerRes <- handler.handle(inputMessage, prefetchKey)
      } yield handlerRes.asInstanceOf[Option[handler.SilentValidationExceptionWrapper]]

      val resultFromOption = Await.result(futureResult, awaitDuration).map(_.silentValidationException).get

      assert(resultFromOption.isInstanceOf[handler.SilentValidationException])
    }

    it("should return a FileNotFoundException given an incorrect file path") {
      val nonExistentFile = "bingress/blah.csv"
      val inputMessage = PrefetchMsg(nonExistentFile, verify = Option(true))

      assertThrows[FileNotFoundException] {
        Await.result(handler.handle(inputMessage, prefetchKey), awaitDuration)
      }
    }

    it("should return an instance of NewAndFoundDoc given a valid message and file exists in the ingress path") {
      val inputMessage = PrefetchMsg(ingressFilenameWithPath)
      val resultFromOption = Await.result(handler.handle(inputMessage, prefetchKey),awaitDuration).get
      assert(resultFromOption.isInstanceOf[handler.NewAndFoundDoc])
    }
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
