package io.mdcatapult.doclib.remote

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.testkit.{ImplicitSender, TestKit}
import com.typesafe.config.{Config, ConfigFactory}
import io.lemonlabs.uri.Uri
import better.files.Dsl._
import org.scalamock.scalatest.MockFactory
import org.scalatest.{AsyncWordSpecLike, BeforeAndAfterAll, Matchers, OptionValues}
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.ExecutionContextExecutor

class ClientIntegrationTest  extends TestKit(ActorSystem("ClientIntegrationTest", ConfigFactory.parseString(
  """
  akka.loggers = ["akka.testkit.TestEventListener"]
  """))) with ImplicitSender
  with AsyncWordSpecLike
  with Matchers
  with BeforeAndAfterAll with OptionValues {
  implicit val config: Config = ConfigFactory.parseString(
    """
      |doclib {
      |  root: "./test"
      |  remote {
      |    target-dir: "remote"
      |    temp-dir: "remote-ingress"
      |  }
      |  local {
      |    target-dir: "local"
      |    temp-dir: "ingress"
      |  }
      |  archive {
      |    target-dir: "archive"
      |  }
      |}
      |mongo {
      |  database: "prefetch-test"
      |  collection: "documents"
      |  connection {
      |    username: "doclib"
      |    password: "doclib"
      |    database: "admin"
      |    hosts: ["localhost"]
      |  }
      |}
    """.stripMargin)
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val executor: ExecutionContextExecutor = scala.concurrent.ExecutionContext.global
  val client = new Client()

  "A valid http URI " should {
    "be downloadable" in {
      val uri: Uri =  Uri.parse("http://www.bbc.co.uk/news")
      val a = client.download(uri)
      assert(a.value.origin.value == "http://www.bbc.co.uk/news")
      assert(a.value.target.value == s"$pwd/${config.getString("doclib.root")}/${config.getString("doclib.remote.target-dir")}/http/www.bbc.co.uk/news.html")
      assert(a.value.source == s"${config.getString("doclib.remote.temp-dir")}/http/www.bbc.co.uk/news.html")
    }
  }

  "A valid FTP URI " should {
    "be downloadable" in {
      val uri = Uri.parse("ftp://ftp.ebi.ac.uk/pub/databases/pmc/suppl/PRIVACY-NOTICE.txt")
      val a = client.download(uri)
      assert(a.value.origin.value == "ftp://ftp.ebi.ac.uk/pub/databases/pmc/suppl/PRIVACY-NOTICE.txt")
      assert(a.value.target.value == s"$pwd/${config.getString("doclib.root")}/${config.getString("doclib.remote.target-dir")}/ftp/ftp.ebi.ac.uk/pub/databases/pmc/suppl/PRIVACY-NOTICE.txt")
      assert(a.value.source == s"${config.getString("doclib.remote.temp-dir")}/ftp/ftp.ebi.ac.uk/pub/databases/pmc/suppl/PRIVACY-NOTICE.txt")
    }
  }


}
