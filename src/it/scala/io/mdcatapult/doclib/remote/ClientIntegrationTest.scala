package io.mdcatapult.doclib.remote

import java.nio.file.{Files, Paths}

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, StreamTcpException}
import akka.testkit.{ImplicitSender, TestKit}
import better.files.Dsl._
import com.typesafe.config.{Config, ConfigFactory}
import io.lemonlabs.uri.Uri
import io.mdcatapult.doclib.util.DirectoryDelete
import org.scalatest.{AsyncWordSpecLike, BeforeAndAfterAll, Matchers, OptionValues}

import scala.concurrent.ExecutionContextExecutor

class ClientIntegrationTest  extends TestKit(ActorSystem("ClientIntegrationTest", ConfigFactory.parseString(
  """
  akka.loggers = ["akka.testkit.TestEventListener"]
  """))) with ImplicitSender
  with AsyncWordSpecLike
  with Matchers
  with BeforeAndAfterAll with OptionValues with DirectoryDelete {
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

  "A valid https URI " should {
    "be downloadable" in {
      val uri: Uri =  Uri.parse("https://www.bbc.co.uk/news")
      val a = client.download(uri)
      assert(a.value.origin.value == "https://www.bbc.co.uk/news")
      assert(a.value.target.value == s"$pwd/${config.getString("doclib.root")}/${config.getString("doclib.remote.target-dir")}/https/www.bbc.co.uk/news.html")
      assert(a.value.source == s"${config.getString("doclib.remote.temp-dir")}/https/www.bbc.co.uk/news.html")
    }
  }

  "An invalid https URI " should {
    "throw an exception" in {
      val uri: Uri =  Uri.parse("https://a.b.c")
      assertThrows[StreamTcpException] {
        client.download(uri)
      }
    }
  }

  "A valid FTP URI " should {
    "be downloadable" in {
      val uri = Uri.parse("ftp://ftp.ebi.ac.uk/pub/databases/pmc/suppl/PRIVACY-NOTICE.txt")
      val a = client.download(uri)
      assert(a.value.origin.value == "ftp://ftp.ebi.ac.uk/pub/databases/pmc/suppl/PRIVACY-NOTICE.txt")
      assert(a.value.target.value == s"$pwd/${config.getString("doclib.root")}/${config.getString("doclib.remote.target-dir")}/ftp/ftp.ebi.ac.uk/pub/databases/pmc/suppl/PRIVACY-NOTICE.txt")
      assert(a.value.source == s"${config.getString("doclib.remote.temp-dir")}/ftp/ftp.ebi.ac.uk/pub/databases/pmc/suppl/PRIVACY-NOTICE.txt")
      assert(Files.exists(Paths.get(s"$pwd/${config.getString("doclib.root")}/${config.getString("doclib.remote.temp-dir")}/ftp/ftp.ebi.ac.uk/pub/databases/pmc/suppl/PRIVACY-NOTICE.txt")))
    }
  }

  override def afterAll(): Unit = {
    // These may or may not exist but are all removed anyway
    deleteDirectories(List(pwd/"test"/"remote-ingress"))
  }


}
