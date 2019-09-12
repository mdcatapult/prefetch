package io.mdcatapult.doclib.remote.adapters

import java.io.File

import com.typesafe.config.{Config, ConfigFactory}
import io.lemonlabs.uri.Uri
import io.mdcatapult.doclib.remote.DownloadResult
import org.scalatest.FlatSpec

import scala.collection.JavaConverters._

class HttpSpec extends FlatSpec {

  implicit val config: Config = ConfigFactory.parseMap(Map[String, Any](
    "prefetch.remote.target-dir" → "./test",
    "prefetch.remote.temp-dir" → "./test"
  ).asJava)

  "A valid HTTPS URL" should "download a file successfully" in {
    val uri = Uri.parse("https://www.google.com/humans.txt")
    val expectedSize = 286
    val result: Option[DownloadResult] = Http.download(uri)
    assert(result.isDefined)
    assert(result.get.isInstanceOf[DownloadResult])
    val file = new File(result.get.source)
    assert(file.exists)
    assert(file.length == expectedSize)
  }

  "A valid HTTP URL" should "download a file successfully" in {
    val uri = Uri.parse("http://www.google.com/robots.txt")
    val expectedSize = 7246
    val result: Option[DownloadResult] = Http.download(uri)
    assert(result.isDefined)
    assert(result.get.isInstanceOf[DownloadResult])
    val file = new File(result.get.source)
    assert(file.exists)
    assert(file.length == expectedSize)
  }
  
}
