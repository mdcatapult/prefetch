package io.mdcatapult.doclib.consumers

import akka.actor._
import akka.pattern.ask
import akka.stream.ActorMaterializer
import akka.testkit.{ImplicitSender, TestKit}
import akka.util.Timeout
import com.spingo.op_rabbit.Message.ConfirmResponse
import com.spingo.op_rabbit.PlayJsonSupport._
import com.spingo.op_rabbit._
import com.typesafe.config.{Config, ConfigFactory}
import io.mdcatapult.doclib.messages.DoclibMsg
import io.mdcatapult.klein.queue.Queue
import org.scalamock.scalatest.MockFactory
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContextExecutor, Future}
import scala.language.postfixOps

class QueueIntegrationTests extends TestKit(ActorSystem("PrefetchHandlerSpec", ConfigFactory.parseString(
  """
  akka.loggers = ["akka.testkit.TestEventListener"]
  """))) with ImplicitSender
  with WordSpecLike
  with Matchers
  with BeforeAndAfterAll with MockFactory with ScalaFutures {

  "A queue" should {
    "be created if it does not exists" in {

      class MessageHandler(downstream: Queue[DoclibMsg]) {
        def handle(msg: DoclibMsg, key: String): Unit = {
        }
      }
      implicit val timeout = Timeout(5 seconds)
      implicit val materializer: ActorMaterializer = ActorMaterializer()
      implicit val executor: ExecutionContextExecutor = scala.concurrent.ExecutionContext.global
      implicit val config: Config = ConfigFactory.load()

      // This first queue is just a convenience to boot the downstream queue
      // Note that we need to include a topic if we want the queue to be created
      val convenienceQueue: Queue[DoclibMsg] = new Queue[DoclibMsg]("a-test-queue", Option(config.getString("op-rabbit.topic-exchange-name")))
      val downstreamQueue: Queue[DoclibMsg] = new Queue[DoclibMsg]("downstream-test-queue", Option(config.getString("op-rabbit.topic-exchange-name")))
      val upstreamQueue: Queue[DoclibMsg] = new Queue[DoclibMsg]("upstream-test-queue", Option("amq.topic"))
      val upstreamSubscription: SubscriptionRef = upstreamQueue.subscribe(new MessageHandler(downstreamQueue).handle)
      val downstreamSubscription: SubscriptionRef = downstreamQueue.subscribe(new MessageHandler(convenienceQueue).handle)
      val result = Await.result(downstreamSubscription.initialized, 5.seconds)

      val downstreamReceived: Future[ConfirmResponse] = (
        downstreamQueue.rabbit ? Message.queue(
          DoclibMsg("downstream"),
          queue = "downstream-test-queue")
        ).mapTo[ConfirmResponse]

      val upstreamReceived: Future[ConfirmResponse] = (
        upstreamQueue.rabbit ? Message.queue(
          DoclibMsg("upstream"),
          queue = "upstream-test-queue")
        ).mapTo[ConfirmResponse]

      whenReady(downstreamReceived) { s =>
        s shouldBe a[Message.Ack]
      }

      whenReady(upstreamReceived) { s =>
        s shouldBe a[Message.Ack]
      }
    }
  }
}
