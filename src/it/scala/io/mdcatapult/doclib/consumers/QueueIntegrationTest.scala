package io.mdcatapult.doclib.consumers

import akka.actor._
import akka.pattern.ask
import akka.testkit.{ImplicitSender, TestKit}
import akka.util.Timeout
import com.spingo.op_rabbit.Message.ConfirmResponse
import com.spingo.op_rabbit.PlayJsonSupport._
import com.spingo.op_rabbit._
import com.typesafe.config.{Config, ConfigFactory}
import io.mdcatapult.doclib.messages.DoclibMsg
import io.mdcatapult.klein.queue.Queue
import org.scalamock.scalatest.MockFactory
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.language.postfixOps

class QueueIntegrationTest extends TestKit(ActorSystem("QueueIntegrationTest", ConfigFactory.parseString(
  """
  akka.loggers = ["akka.testkit.TestEventListener"]
  """))) with ImplicitSender
  with AnyWordSpecLike
  with Matchers
  with BeforeAndAfterAll with MockFactory with ScalaFutures with Eventually {

  "A queue" ignore {
    "be created if it does not exist" in {

      var messagesFromQueue = List[DoclibMsg]()

      implicit val timeout: Timeout = Timeout(5 seconds)
      implicit val config: Config = ConfigFactory.load()

      // Note that we need to include a topic if we want the queue to be created
      val consumerName = Option(config.getString("op-rabbit.topic-exchange-name"))
      val queueName = "a-test-queue"

      val queue = Queue[DoclibMsg](queueName, consumerName)

      val subscription: SubscriptionRef =
        queue.subscribe((msg: DoclibMsg) => messagesFromQueue ::= msg)

      Await.result(subscription.initialized, 5.seconds)

      val messageSent: Future[ConfirmResponse] = (
        queue.rabbit ? Message.queue(
          DoclibMsg("test message"),
          queue = queueName)
        ).mapTo[ConfirmResponse]

      whenReady(messageSent) { response =>
        response shouldBe a[Message.Ack]
      }

      eventually {
        messagesFromQueue should contain (DoclibMsg("test message"))
      }
    }
  }
}

