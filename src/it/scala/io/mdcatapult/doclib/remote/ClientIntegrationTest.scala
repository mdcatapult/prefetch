package io.mdcatapult.doclib.remote

import java.nio.file.{Files, Paths}

import akka.actor.ActorSystem
import akka.stream.{Materializer, StreamTcpException}
import akka.testkit.{ImplicitSender, TestKit}
import better.files.Dsl._
import com.typesafe.config.{Config, ConfigFactory}
import io.lemonlabs.uri.Uri
import io.mdcatapult.doclib.util.DirectoryDelete
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpecLike
import org.scalatest.{BeforeAndAfterAll, OptionValues}

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

  implicit val m: Materializer = Materializer(system)
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

  "A valid http URI that redirects " should {
    "be downloadable" in {
      val uri: Uri =  Uri.parse("http://www.tandfonline.com/doi/pdf/10.4081/ijas.2015.3712?needAccess=true")
      val a = client.download(uri)
      assert(a.value.origin.value == "http://www.tandfonline.com/doi/pdf/10.4081/ijas.2015.3712?needAccess=true")
      assert(a.value.target.value == s"$pwd/${config.getString("doclib.root")}/${config.getString("doclib.remote.target-dir")}/http/www.tandfonline.com/doi/pdf/10.4081/ijas.2015.f3e24e247796d0e8aadc4607bfdfdbe4.3712")
      assert(a.value.source == s"${config.getString("doclib.remote.temp-dir")}/http/www.tandfonline.com/doi/pdf/10.4081/ijas.2015.f3e24e247796d0e8aadc4607bfdfdbe4.3712")
    }
  }

  "A valid https URI that redirects " should {
    "be downloadable" in {
      val uri: Uri =  Uri.parse("https://www.tandfonline.com/doi/pdf/10.4081/ijas.2015.3712?needAccess=true")
      val a = client.download(uri)
      assert(a.value.origin.value == "https://www.tandfonline.com/doi/pdf/10.4081/ijas.2015.3712?needAccess=true")
      assert(a.value.target.value == s"$pwd/${config.getString("doclib.root")}/${config.getString("doclib.remote.target-dir")}/https/www.tandfonline.com/doi/pdf/10.4081/ijas.2015.f3e24e247796d0e8aadc4607bfdfdbe4.3712")
      assert(a.value.source == s"${config.getString("doclib.remote.temp-dir")}/https/www.tandfonline.com/doi/pdf/10.4081/ijas.2015.f3e24e247796d0e8aadc4607bfdfdbe4.3712")
    }
  }

  "A valid https URI from Wiley that redirects " should {
    "be downloadable" in {
      val uri: Uri =  Uri.parse("https://onlinelibrary.wiley.com/doi/pdf/10.1002/9780470998137.indauth")
      val a = client.download(uri)
      assert(a.value.origin.value == "https://onlinelibrary.wiley.com/doi/pdf/10.1002/9780470998137.indauth")
      assert(a.value.target.value == s"$pwd/${config.getString("doclib.root")}/${config.getString("doclib.remote.target-dir")}/https/onlinelibrary.wiley.com/doi/pdf/10.1002/9780470998137.indauth")
      assert(a.value.source == s"${config.getString("doclib.remote.temp-dir")}/https/onlinelibrary.wiley.com/doi/pdf/10.1002/9780470998137.indauth")
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
    deleteDirectories(List(pwd/"test"/"remote-ingress", pwd/"test"/"local", pwd/"test"/"archive", pwd/"test"/"ingress", pwd/"test"/"local", pwd/"test"/"remote"))
  }


}
