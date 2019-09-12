package io.mdcatapult.doclib.remote

import java.io.File

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.FlatSpec
import io.lemonlabs.uri._
import io.mdcatapult.doclib.models.PrefetchOrigin

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContextExecutor}
import scala.io.Source

class ClientSpec extends FlatSpec{
  val wsConfFile = getClass.getResource("/test/ws.conf")
  val wsConfig = ConfigFactory.parseURL(wsConfFile)
  implicit val config: Config = ConfigFactory.parseString(
    """
      |prefetch {
      |  remote {
      |    target-dir: "./test"
      |    temp-dir: "./test"
      |  }
      |}
    """.stripMargin).withFallback(wsConfig)
  implicit val system: ActorSystem = ActorSystem("scalatest", config)
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val executor: ExecutionContextExecutor = system.getDispatcher
  val client = new Client()

  "A valid http URL that redirects to https" should "resolve to a valid Prefetch Origin" in {
    val result = Await.result(client.resolve(Uri.parse("http://news.bbc.co.uk")), Duration.Inf)
    assert(result.scheme == "https")
    assert(result.uri.get.schemeOption.get == "https")
    assert(result.uri.get.toUrl.hostOption.get.toString == "www.bbc.co.uk")
    assert(result.uri.get.toUrl.path.toAbsolute.toString == "/news")
  }

  "A valid ftp URL" should "resolve to a valid Prefetch Origin" in {
    val source = Uri.parse("ftp://ftp.ebi.ac.uk/pub/databases/pmc/suppl/PRIVACY-NOTICE.txt")
    val result = Await.result(client.resolve(source), Duration.Inf)
    assert(result.scheme == "ftp")
    assert(result.uri.get == source)
  }
}
