package io.mdcatapult.doclib.remote.adapters

import java.io.File

import better.files.Dsl.pwd
import com.typesafe.config.{Config, ConfigFactory}
import io.lemonlabs.uri.Uri
import io.mdcatapult.doclib.remote.DownloadResult
import io.mdcatapult.doclib.util.DirectoryDelete
import org.scalatest.{BeforeAndAfterAll, FlatSpec}

class FtpIntegrationTest extends FlatSpec with DirectoryDelete with BeforeAndAfterAll {

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

  "A valid anonymous FTP URL" should "download a file successfully" in {
    val uri = Uri.parse("ftp://ftp.ebi.ac.uk/pub/databases/pmc/suppl/PRIVACY-NOTICE.txt")
    val result: Option[DownloadResult] = Ftp.download(uri)
    assert(result.isDefined)
    assert(result.get.isInstanceOf[DownloadResult])
    val file = new File(s"${config.getString("doclib.root")}/${result.get.source}")
    assert(file.exists)
  }

  "A valid FTP URL with credentials" should "parse ok" in {
    // Test username/password. Doesn't matter if it downloads
    val uri = Uri.parse("ftp://user:password@ftp.ebi.ac.uk/pub/databases/pmc/suppl/PRIVACY-NOTICE.txt")
    intercept[Exception] {
      Ftp.download(uri)
    }
  }

  override def afterAll {
    // These may or may not exist but are all removed anyway
    deleteDirectories(List(pwd/"test"/"ftp-test"))
  }

}