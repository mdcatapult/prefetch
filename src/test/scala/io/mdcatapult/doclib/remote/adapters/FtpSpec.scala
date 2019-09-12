package io.mdcatapult.doclib.remote.adapters

import java.io.File
import java.nio.file.{Files, Paths}

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.typesafe.config.{Config, ConfigFactory}
import io.lemonlabs.uri.Uri
import io.mdcatapult.doclib.remote.DownloadResult
import org.scalatest.FlatSpec

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContextExecutor

class FtpSpec extends FlatSpec {

  implicit val config: Config = ConfigFactory.parseMap(Map[String, Any](
    "prefetch.remote.target-dir" → "./test",
    "prefetch.remote.temp-dir" → "./test"
  ).asJava)

  "A valid anonymous FTP URL" should "download a file successfully" in {
    val uri = Uri.parse("ftp://ftp.ebi.ac.uk/pub/databases/pmc/suppl/PRIVACY-NOTICE.txt")
    val expectedSize = 426
    val result: Option[DownloadResult] = Ftp.download(uri)
    assert(result.isDefined)
    assert(result.get.isInstanceOf[DownloadResult])
    val file = new File(result.get.source)
    assert(file.exists)
    assert(file.length == expectedSize)
  }

}
