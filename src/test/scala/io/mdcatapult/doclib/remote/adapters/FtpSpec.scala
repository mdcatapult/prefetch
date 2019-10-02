package io.mdcatapult.doclib.remote.adapters

import java.io.File

import better.files.Dsl.pwd
import com.typesafe.config.{Config, ConfigFactory}
import io.lemonlabs.uri.Uri
import io.mdcatapult.doclib.TestDirectoryDelete
import io.mdcatapult.doclib.remote.DownloadResult
import org.scalatest.{BeforeAndAfterAll, FlatSpec}

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

  "A broken FTP URL" should "fail" in {
    val uri = Uri.parse("ftp://a.b.c/something")
    intercept[Exception] {
      Ftp.download(uri)
    }
  }

}
