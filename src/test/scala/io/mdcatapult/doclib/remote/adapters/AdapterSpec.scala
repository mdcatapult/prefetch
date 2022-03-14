package io.mdcatapult.doclib.remote.adapters

import akka.stream.Materializer
import com.typesafe.config.{Config, ConfigFactory}
import io.lemonlabs.uri.Uri
import io.mdcatapult.doclib.models.Origin
import io.mdcatapult.doclib.remote.DownloadResult
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.Future

class AdapterSpec extends AnyFlatSpec with Matchers {

  implicit val config: Config = ConfigFactory.parseString(
    """
      |doclib {
      |  root: "/test"
      |  local {
      |    target-dir: "local"
      |    temp-dir: "ingress"
      |  }
      |  remote {
      |    target-dir: "remote"
      |    temp-dir: "remote-ingress"
      |  }
      |}
    """.stripMargin)

  private val dummy = new Adapter {
    def unapply(origin: Origin): Boolean = true
    def download(origin: Origin)(implicit config: Config, m: Materializer): Future[Option[DownloadResult]] = Future.successful(None)
  }
  import dummy.generateFilePath

  "A URL with no file extension suffix" should  "generate a file path ending with file extension suffix" in {
    val origin = Origin(
      scheme = "https",
      uri = Uri.parseOption("https://www.bbc.co.uk/news")
    )
    val result = generateFilePath(origin, Some("remote"), None, None)
    assert(result == "remote/https/www.bbc.co.uk/news.html")
  }

  it should  "generate from content type a file path ending when a content type is defined" in {
    val origin = Origin(
      scheme = "https",
      uri = Uri.parseOption("https://www.bbc.co.uk/news"),
    )
    val result = generateFilePath(origin, Some("remote"), None, Some("application/pdf"))
    assert(result == "remote/https/www.bbc.co.uk/news.pdf")
  }

  "A URL with query string" should  "generate a file path with an md5 hash before the file extension" in {
    val origin = Origin(
      scheme = "https",
      uri = Uri.parseOption("https://www.bbc.co.uk/news?page=1"),
    )
    val result = generateFilePath(origin, Some("remote"), None, None)
    assert(result == "remote/https/www.bbc.co.uk/news.1a8c14b8c6351d0699ed8db13dcde382.html")
  }

  "A URL with terminating in a / separator" should  "generate a file path ending with index.html" in {
    val origin = Origin(
      scheme = "https",
      uri = Uri.parseOption("https://www.bbc.co.uk/news/"),
    )
    val result = generateFilePath(origin, Some("remote"), None, None)
    assert(result == "remote/https/www.bbc.co.uk/news/index.html")
  }

  "A URL without a path" should  "generate a file path ending in index.html" in {
    val origin = Origin(
      scheme = "https",
      uri = Uri.parseOption("https://www.bbc.co.uk"),
    )
    val result = generateFilePath(origin, Some("remote"), None, None)
    assert(result == "remote/https/www.bbc.co.uk/index.html")
  }

  "A URL without a path but with a query string" should  "generate a file path ending with md5 hash before index.html" in {
    val origin = Origin(
      scheme = "https",
      uri = Uri.parseOption("https://www.bbc.co.uk?page=1"),
    )
    val result = generateFilePath(origin, Some("remote"), None, None)
    assert(result == "remote/https/www.bbc.co.uk/index.1a8c14b8c6351d0699ed8db13dcde382.html")
  }

  "A URL without a sub path but with terminating /" should  "generate file path ending in index.html" in {
    val origin = Origin(
      scheme = "https",
      uri = Uri.parseOption("https://www.bbc.co.uk/"),
    )
    val result = generateFilePath(origin, Some("remote"), None, None)
    assert(result == "remote/https/www.bbc.co.uk/index.html")
  }

  "Calling Adapter.generateFilePath with a replacement file name" should "generate file path with file name replacing the end path" in {
    val origin = Origin(
      scheme = "https",
      uri = Uri.parseOption("https://www.bbc.co.uk/news/world-51235105"),
    )
    val result = generateFilePath(origin, Some("remote"), Some("coronavirus.pdf"), None)
    result should be("remote/https/www.bbc.co.uk/news/world-51235105/coronavirus.pdf")
  }

  it should "include query hash when there are query params" in {
    val origin = Origin(
      scheme = "https",
      uri = Uri.parseOption("https://www.bbc.co.uk/news/world-51235105?page=1"),
    )
    val result = generateFilePath(origin, Some("remote"), Some("coronavirus.pdf"), None)
    result should be("remote/https/www.bbc.co.uk/news/world-51235105/coronavirus.1a8c14b8c6351d0699ed8db13dcde382.pdf")
  }

  it should "include query hash with content type at end of math path when there are query params and content type is defined" in {
    val origin = Origin(
      scheme = "https",
      uri = Uri.parseOption("https://www.bbc.co.uk/news/world-51235105?page=1"),
    )
    val result = generateFilePath(origin, Some("remote"), Some("coronavirus.pdf"), Some("text/html"))
    result should be("remote/https/www.bbc.co.uk/news/world-51235105/coronavirus.1a8c14b8c6351d0699ed8db13dcde382.pdf")
  }

  it should "append file name to path when path ends in /" in {
    val origin = Origin(
      scheme = "https",
      uri = Uri.parseOption("https://www.bbc.co.uk/news/"),
    )
    val result = generateFilePath(origin, Some("remote"), Some("virus.pdf"), None)
    result should be("remote/https/www.bbc.co.uk/news/virus.pdf")
  }

  "A URL with a content disposition header" should  "use the filename given by the header" in {
    val headers = Map("Content-Disposition" -> Seq("inline; filename=edinbmedj74939-0070a.pdf"))
    val origin = Origin(
      scheme = "https",
      uri = Uri.parseOption("https://www.bbc.co.uk/news"),
      headers = Option apply headers
    )
    val result = generateFilePath(origin, Some("remote"), None, None)
    assert(result == "remote/https/www.bbc.co.uk/news/edinbmedj74939-0070a.pdf")
  }

  it should "find the filename when the header name is not capitalised" in {
    val headers = Map("content-disposition" -> Seq("inline; filename=edinbmedj74939-0070a.pdf"))
    val origin = Origin(
      scheme = "https",
      uri = Uri.parseOption("https://www.bbc.co.uk/news"),
      headers = Option apply headers
    )
    val result = generateFilePath(origin, Some("remote"), None, None)
    assert(result == "remote/https/www.bbc.co.uk/news/edinbmedj74939-0070a.pdf")
  }

  it should "find the filename when it is not the first in the sequence of header values" in {
    val headers = Map("Content-Disposition" -> Seq("", "inline; filename=edinbmedj74939-0070a.pdf"))
    val origin = Origin(
      scheme = "https",
      uri = Uri.parseOption("https://www.bbc.co.uk/news"),
      headers = Option apply headers
    )
    val result = generateFilePath(origin, Some("remote"), None, None)
    assert(result == "remote/https/www.bbc.co.uk/news/edinbmedj74939-0070a.pdf")
  }

  it should "find the filename when the filename is all caps" in {
    val headers = Map("Content-Disposition" -> Seq("inline; FILENAME=edinbmedj74939-0070a.pdf"))
    val origin = Origin(
      scheme = "https",
      uri = Uri.parseOption("https://www.bbc.co.uk/news"),
      headers = Option apply headers
    )
    val result = generateFilePath(origin, Some("remote"), None, None)
    assert(result == "remote/https/www.bbc.co.uk/news/edinbmedj74939-0070a.pdf")
  }

  it should "trim leading and trailing quotes from the filename" in {
    val headers = Map("Content-Disposition" -> Seq("inline; filename=\"edinbmedj74939-0070a.pdf\""))
    val origin = Origin(
      scheme = "https",
      uri = Uri.parseOption("https://www.bbc.co.uk/news"),
      headers = Option apply headers
    )
    val result = generateFilePath(origin, Some("remote"), None, None)
    assert(result == "remote/https/www.bbc.co.uk/news/edinbmedj74939-0070a.pdf")
  }

  it should "trim a leading utf-8 identifier from the filename" in {
    val headers = Map("Content-Disposition" -> Seq("inline; filename=utf-8''edinbmedj74939-0070a.pdf"))
    val origin = Origin(
      scheme = "https",
      uri = Uri.parseOption("https://www.bbc.co.uk/news"),
      headers = Option apply headers
    )
    val result = generateFilePath(origin, Some("remote"), None, None)
    assert(result == "remote/https/www.bbc.co.uk/news/edinbmedj74939-0070a.pdf")
  }

  it should "trim a leading UTF-8 identifier from the filename" in {
    val headers = Map("Content-Disposition" -> Seq("inline; filename=UTF-8''edinbmedj74939-0070a.pdf"))
    val origin = Origin(
      scheme = "https",
      uri = Uri.parseOption("https://www.bbc.co.uk/news"),
      headers = Option apply headers
    )
    val result = generateFilePath(origin, Some("remote"), None, None)
    assert(result == "remote/https/www.bbc.co.uk/news/edinbmedj74939-0070a.pdf")
  }

  it should "append file name to path when path ends in /" in {
    val headers = Map("Content-Disposition" -> Seq("inline; filename=edinbmedj74939-0070a.pdf"))
    val origin = Origin(
      scheme = "https",
      uri = Uri.parseOption("https://www.bbc.co.uk/news/"),
      headers = Option apply headers
    )
    val result = generateFilePath(origin, Some("remote"), None, None)
    assert(result == "remote/https/www.bbc.co.uk/news/edinbmedj74939-0070a.pdf")
  }

  it should "use the file name instead of index.html when there is no path" in {
    val headers = Map("Content-Disposition" -> Seq("inline; filename=edinbmedj74939-0070a.pdf"))
    val origin = Origin(
      scheme = "https",
      uri = Uri.parseOption("https://www.bbc.co.uk"),
      headers = Option apply headers
    )
    val result = generateFilePath(origin, Some("remote"), None, None)
    assert(result == "remote/https/www.bbc.co.uk/edinbmedj74939-0070a.pdf")
  }

  it should "use the file name and ignore the path end and query parameters" in {
    val headers = Map("Content-Disposition" -> Seq("inline; filename=edinbmedj74939-0070a.pdf"))
    val origin = Origin(
      scheme = "https",
      uri = Uri.parseOption("https://www.bbc.co.uk/world-51235105?page=1"),
      headers = Option apply headers
    )
    val result = generateFilePath(origin, Some("remote"), None, None)
    assert(result == "remote/https/www.bbc.co.uk/world-51235105/edinbmedj74939-0070a.pdf")
  }

  it should "be overrriden by a given filename" in {
    val headers = Map("Content-Disposition" -> Seq("inline; filename=edinbmedj74939-0070a.pdf"))
    val origin = Origin(
      scheme = "https",
      uri = Uri.parseOption("https://www.bbc.co.uk/world-51235105"),
      headers = Option apply headers
    )
    val result = generateFilePath(origin, Some("remote"), Some("coronavirus.pdf"), None)
    assert(result == "remote/https/www.bbc.co.uk/world-51235105/coronavirus.pdf")
  }

  it should "be overrriden by a given filename and queryhash" in {
    val headers = Map("Content-Disposition" -> Seq("inline; filename=edinbmedj74939-0070a.pdf"))
    val origin = Origin(
      scheme = "https",
      uri = Uri.parseOption("https://www.bbc.co.uk/world-51235105?page=1"),
      headers = Option apply headers
    )
    val result = generateFilePath(origin, Some("remote"), Some("coronavirus.pdf"), None)
    assert(result == "remote/https/www.bbc.co.uk/world-51235105/coronavirus.1a8c14b8c6351d0699ed8db13dcde382.pdf")
  }

  it should "use the http location header if available" in {
    val headers = Map("Location" -> Seq("https://a.b.c.com"))
    val origin = Origin(
      scheme = "https",
      uri = Uri.parseOption("https://www.bbc.co.uk/edinbmedj74939-0070a.pdf"),
      headers = Option apply headers
    )
    val result = generateFilePath(origin, Some("remote"), None, None)
    assert(result == "remote/https/a.b.c.com/edinbmedj74939-0070a.pdf")
  }

  it should "use a relative http location header if available" in {
    val headers = Map("Location" -> Seq("/somewhere"))
    val origin = Origin(
      scheme = "https",
      uri = Uri.parseOption("https://www.bbc.co.uk/edinbmedj74939-0070a.pdf"),
      headers = Option apply headers
    )
    val result = generateFilePath(origin, Some("remote"), None, None)
    assert(result == "remote/https/www.bbc.co.uk/somewhere/edinbmedj74939-0070a.pdf")
  }

  it should "ignore an location header of /" in {
    val headers = Map("Location" -> Seq("/"))
    val origin = Origin(
      scheme = "https",
      uri = Uri.parseOption("https://www.bbc.co.uk/edinbmedj74939-0070a.pdf"),
      headers = Option apply headers
    )
    val result = generateFilePath(origin, Some("remote"), None, None)
    assert(result == "remote/https/www.bbc.co.uk/edinbmedj74939-0070a.pdf")
  }

  it should "ignore an empty location header" in {
    val headers = Map("Location" -> Seq(""))
    val origin = Origin(
      scheme = "https",
      uri = Uri.parseOption("https://www.bbc.co.uk/edinbmedj74939-0070a.pdf"),
      headers = Option apply headers
    )
    val result = generateFilePath(origin, Some("remote"), None, None)
    assert(result == "remote/https/www.bbc.co.uk/edinbmedj74939-0070a.pdf")
  }

  it should "use the content-disposition with original origin path even if location header is available" in {
    val headers = Map("Location" -> Seq("https://a.b.c.com"), "Content-Disposition" -> Seq("inline; filename=edinbmedj74939-0070a.pdf"))
    val origin = Origin(
      scheme = "https",
      uri = Uri.parseOption("https://www.bbc.co.uk/world-51235105?page=1"),
      headers = Option apply headers
    )
    val result = generateFilePath(origin, Some("remote"), None, None)
    assert(result == "remote/https/www.bbc.co.uk/world-51235105/edinbmedj74939-0070a.pdf")
  }

}
