package io.mdcatapult.doclib.remote.adapters

import java.io.File

import com.typesafe.config.{Config, ConfigFactory}
import io.lemonlabs.uri.Uri
import io.mdcatapult.doclib.remote.DownloadResult
import org.scalatest.FlatSpec

import scala.collection.JavaConverters._

class FtpSpec extends FlatSpec {

  implicit val config: Config = ConfigFactory.parseMap(Map[String, Any](
    "prefetch.remote.target-dir" → "./test",
    "prefetch.remote.temp-dir" → "./test"
  ).asJava)

  "A valid anonymous FTP URL" should "download a file successfully" in {
    val uri = Uri.parse("ftp://ftp.ebi.ac.uk/pub/databases/pmc/suppl/PRIVACY-NOTICE.txt")
    //val expectedSize = 426
    val result: Option[DownloadResult] = Ftp.download(uri)
    assert(result.isDefined)
    assert(result.get.isInstanceOf[DownloadResult])
    val file = new File(result.get.source)
    assert(file.exists)
    //assert(file.length == expectedSize)
  }

  "A broken FTP URL" should "fail" in {
    val uri = Uri.parse("ftp://a.b.c/something")
    val caught = intercept[Exception] {
      Ftp.download(uri)
    }
    assert(caught.getMessage == "Could not process ftp://a.b.c/something.a.b.c: Name or service not known")
  }

}
