package io.mdcatapult.doclib.remote.adapters

import com.typesafe.config.{Config, ConfigFactory}
import io.lemonlabs.uri.Uri
import org.scalatest.FlatSpec

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
