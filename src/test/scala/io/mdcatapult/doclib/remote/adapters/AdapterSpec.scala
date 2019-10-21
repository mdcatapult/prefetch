package io.mdcatapult.doclib.remote.adapters

import com.typesafe.config.{Config, ConfigFactory}
import io.lemonlabs.uri.Uri
import io.mdcatapult.doclib.remote.DownloadResult
import org.scalatest.FlatSpec

class AdapterSpec extends FlatSpec {

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

    class Dummy extends Adapter {
      def unapply(uri: Uri)(implicit config: Config): Option[DownloadResult] = None
      def download(uri: Uri)(implicit config: Config): Option[DownloadResult] = None
    }

    "A URL with no file extension suffix" should  "generate a file path ending with file extension suffix" in {
      val result = new Dummy().generateFilePath(Uri.parse("https://www.bbc.co.uk/news"), Some("remote"))
      assert(result == "remote/https/www.bbc.co.uk/news.html")
    }

    "A URL with query string" should  "generate a file path with an md5 hash before the file extension" in {
      val result = new Dummy().generateFilePath(Uri.parse("https://www.bbc.co.uk/news?page=1"), Some("remote"))
      assert(result == "remote/https/www.bbc.co.uk/news.1a8c14b8c6351d0699ed8db13dcde382.html")
    }

    "A URL with terminating in a / separator" should  "generate a file path ending with index.html" in {
      val result = new Dummy().generateFilePath(Uri.parse("https://www.bbc.co.uk/news/"), Some("remote"))
      assert(result == "remote/https/www.bbc.co.uk/news/index.html")
    }
  "A URL without a path" should  "generate a file path ending in index.html" in {
    val result = new Dummy().generateFilePath(Uri.parse("https://www.bbc.co.uk"), Some("remote"))
    assert(result == "remote/https/www.bbc.co.uk/index.html")
  }

  "A URL without a path but with a query string" should  "generate a file path ending with md5 hash before index.html" in {
    val result = new Dummy().generateFilePath(Uri.parse("https://www.bbc.co.uk?page=1"), Some("remote"))
    assert(result == "remote/https/www.bbc.co.uk/index.1a8c14b8c6351d0699ed8db13dcde382.html")
  }

  "A URL without a sub path but with terminating /" should  "generate file path ending in index.html" in {
    val result = new Dummy().generateFilePath(Uri.parse("https://www.bbc.co.uk/"), Some("remote"))
    assert(result == "remote/https/www.bbc.co.uk/index.html")
  }

}
