package io.mdcatapult.doclib.remote.adapters

import java.io.File

import akka.actor.ActorSystem
import akka.stream.Materializer
import better.files.Dsl.pwd
import com.typesafe.config.{Config, ConfigFactory}
import io.lemonlabs.uri.Uri
import io.mdcatapult.doclib.models.Origin
import io.mdcatapult.doclib.remote.DownloadResult
import io.mdcatapult.doclib.util.DirectoryDelete
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

class FtpIntegrationTest extends AnyFlatSpec with DirectoryDelete with BeforeAndAfterAll {

  implicit val config: Config = ConfigFactory.parseString(
    s"""
       |doclib {
       |  root: "${pwd/"test"/"ftp-test"}"
       |  remote {
       |    target-dir: "remote"
       |    temp-dir: "remote-ingress"
       |  }
       |}
    """.stripMargin)

  private val system = ActorSystem("ftp-integration-spec")
  implicit val m: Materializer = Materializer(system)

  "A valid anonymous FTP URL" should "download a file successfully" in {
    val origin: Origin = Origin("ftp", uri = Uri.parseOption("ftp://ftp.ebi.ac.uk/pub/databases/pmc/suppl/PRIVACY-NOTICE.txt"))
    val result: Option[DownloadResult] = Ftp.download(origin)
    assert(result.isDefined)
    assert(result.get.isInstanceOf[DownloadResult])
    val file = new File(s"${config.getString("doclib.root")}/${result.get.source}")
    assert(file.exists)
  }

  "A valid FTP URL with credentials" should "parse ok" in {
    // Test username/password. Doesn't matter if it downloads
    val origin = Origin("ftp", uri = Uri.parseOption("ftp://user:password@ftp.ebi.ac.uk/pub/databases/pmc/suppl/PRIVACY-NOTICE.txt"))
    intercept[Exception] {
      Ftp.download(origin)
    }
  }

  override def afterAll(): Unit = {
    // These may or may not exist but are all removed anyway
    deleteDirectories(
      List(
        pwd/"test"/"ftp-test",
        pwd/"test"/"remote-ingress",
        pwd/"test"/"local",
        pwd/"test"/"archive",
        pwd/"test"/"ingress",
        pwd/"test"/"local",
        pwd/"test"/"remote"
      ))
  }

}
