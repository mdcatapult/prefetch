package io.mdcatapult.doclib.remote.adapters

import java.io.File

import com.typesafe.config.{Config, ConfigFactory}
import io.lemonlabs.uri.Uri
import io.mdcatapult.doclib.remote.DownloadResult
import org.scalatest.FlatSpec

import scala.collection.JavaConverters._

class FtpSpec extends FlatSpec {

  implicit val config: Config = ConfigFactory.parseString(
    """
      |doclib {
      |  root: "./test"
      |  remote {
      |    target-dir: "remote"
      |    temp-dir: "remote-ingress"
      |  }
      |}
    """.stripMargin)

  "A valid anonymous FTP URL" should "download a file successfully" in {
    val uri = Uri.parse("ftp://ftp.ebi.ac.uk/pub/databases/pmc/suppl/PRIVACY-NOTICE.txt")
    //val expectedSize = 426
    val result: Option[DownloadResult] = Ftp.download(uri)
    assert(result.isDefined)
    assert(result.get.isInstanceOf[DownloadResult])
    val file = new File(s"${config.getString("doclib.root")}/${result.get.source}")
    assert(file.exists)
    //assert(file.length == expectedSize)
  }

  "A broken FTP URL" should "fail" in {
    val uri = Uri.parse("ftp://a.b.c/something")
    intercept[Exception] {
      Ftp.download(uri)
    }
  }

}
