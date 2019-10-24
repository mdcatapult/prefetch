package io.mdcatapult.doclib.remote.adapters

import java.io.File

import com.typesafe.config.{Config, ConfigFactory}
import io.lemonlabs.uri.Uri
import io.mdcatapult.doclib.remote.DownloadResult
import org.scalatest.FlatSpec

class HttpSpec extends FlatSpec {

  implicit val config: Config = ConfigFactory.parseString(
    """
      |doclib {
      |  root: ./test
      |  remote {
      |    target-dir: "remote"
      |    temp-dir: "remote-ingress"
      |  }
      |}
    """.stripMargin)

  "A valid HTTPS URL" should "download a file successfully" in {
    val uri = Uri.parse("https://www.google.com/humans.txt")
    //val expectedSize = 286
    val result: Option[DownloadResult] = Http.download(uri)
    assert(result.isDefined)
    assert(result.get.isInstanceOf[DownloadResult])
    val file = new File(s"${config.getString("doclib.root")}/${result.get.source}")
    assert(file.exists)
    //assert(file.length == expectedSize)
  }

  "A valid HTTP URL" should "download a file successfully" in {
    val uri = Uri.parse("http://www.google.com/robots.txt")
    //val expectedSize = 7246
    val result: Option[DownloadResult] = Http.download(uri)
    assert(result.isDefined)
    assert(result.get.isInstanceOf[DownloadResult])
    val file = new File(s"${config.getString("doclib.root")}/${result.get.source}")
    assert(file.exists)
    //assert(file.length == expectedSize)
  }

  "An URL to nowhere" should "throw an Exception" in {
    val uri = Uri.parse("http://www.a.b.c/something")
    val caught = intercept[Exception] {
      Http.download(uri)
    }
    assert(caught.getMessage == "Unable to retrieve headers for URL http://www.a.b.c/something")
  }

  "A valid URL with unknown file" should "throw an Exception" in {
    val uri = Uri.parse("http://www.google.com/this-is-an-invalid-file.pdf")
    val caught = intercept[Exception] {
      Http.download(uri)
    }
    assert(caught.getMessage == "Unable to process URL with resolved status code of 404")
  }

  "A filename that is shorter than 255 chars" should "not be changed" in {
    val origUri = Uri.parse("http://eprints.hud.ac.uk/10819/1/MorralNew.pdf")
    val fileName = "MorralNew.pdf"
    val hashName = Http.hashOrOriginal(origUri, fileName)
    assert(hashName == fileName)
  }

  "A uri with calculated filename that is longer than 255 chars with query params" should "be hashed" in {
    val origUri = Uri.parse("http://orbit.dtu.dk/en/publications/dual-nicotinic-acetylcholine-receptor-42-antagonists7-agonists-synthesis-docking-studies-and-pharmacological-evaluation-of-tetrahydroisoquinolines-and-tetrahydroisoquinolinium-salts(040536e1-22a7-47c7-bee9-51e53543ff04).pdf?nofollow=true&rendering=author")
    val fileName = "dual-nicotinic-acetylcholine-receptor-42-antagonists7-agonists-synthesis-docking-studies-and-pharmacological-evaluation-of-tetrahydroisoquinolines-and-tetrahydroisoquinolinium-salts(040536e1-22a7-47c7-bee9-51e53543ff04).74bdbe0d010151d2d42f6768eca1290c.pdf"
    val actualName = "dual-nicotinic-acetylcholine-receptor-42-antagonists7-agonists-synthesis-docking-studies-and-pharmacological-evaluation-of-tetrahydroisoquinolines-and-tetrahydroisoquinolinium-salts(040536e1-22a7-47c7-bee9-51e53543ff04)"
    val hashName = Http.hashOrOriginal(origUri, fileName)
    val fileNameHash = Http.md5HashString(actualName)
    assert(hashName == s"$fileNameHash.74bdbe0d010151d2d42f6768eca1290c.pdf")
  }

  "A uri with filename longer than 255 chars with no query params" should "be hashed" in {
    val origUri = Uri.parse("http://orbit.dtu.dk/en/publications/dual-nicotinic-acetylcholine-receptor-42-antagonists7-agonists-synthesis-docking-studies-and-pharmacological-evaluation-of-tetrahydroisoquinolines-and-tetrahydroisoquinolinium-salts(040536e1-22a7-47c7-bee9-51e53543ff04)-this-is-way-longer-than-it-really-should-be.pdf")
    val fileName = "dual-nicotinic-acetylcholine-receptor-42-antagonists7-agonists-synthesis-docking-studies-and-pharmacological-evaluation-of-tetrahydroisoquinolines-and-tetrahydroisoquinolinium-salts(040536e1-22a7-47c7-bee9-51e53543ff04)-this-is-way-longer-than-it-really-should-be.pdf"
    val hashName = Http.hashOrOriginal(origUri, fileName)
    val fileNameHash = Http.md5HashString(fileName.replace(".pdf", ""))
    assert(hashName == s"$fileNameHash.pdf")
  }

  "A uri with filename longer than 255 chars with no query params and no extension" should "be hashed" in {
    val origUri = Uri.parse("http://orbit.dtu.dk/en/publications/dual-nicotinic-acetylcholine-receptor-42-antagonists7-agonists-synthesis-docking-studies-and-pharmacological-evaluation-of-tetrahydroisoquinolines-and-tetrahydroisoquinolinium-salts(040536e1-22a7-47c7-bee9-51e53543ff04)-this-is-way-longer-than-it-really-should-be")
    val fileName = "dual-nicotinic-acetylcholine-receptor-42-antagonists7-agonists-synthesis-docking-studies-and-pharmacological-evaluation-of-tetrahydroisoquinolines-and-tetrahydroisoquinolinium-salts(040536e1-22a7-47c7-bee9-51e53543ff04)-this-is-way-longer-than-it-really-should-be"
    val hashName = Http.hashOrOriginal(origUri, fileName)
    val fileNameHash = Http.md5HashString(fileName)
    assert(hashName == s"$fileNameHash")
  }

  "A url that has no path" should "not be changed" in {
    val origUri = Uri.parse("www.bbc.co.uk")
    val fileName = ""
    val hashName = Http.hashOrOriginal(origUri, fileName)
    assert(hashName == fileName)
  }

  "A url with a filename that is shorter than 255 chars and has a path" should "not be changed" in {
    val origUri = Uri.parse("www.bbc.co.uk")
    val fileName = ""
    val hashName = Http.hashOrOriginal(origUri, fileName)
    assert(hashName == fileName)
  }

  "A url filename that is shorter than 255 chars and has path" should "not be changed" in {
    val origUri = Uri.parse("www.bbc.co.uk/news")
    val fileName = "news"
    val hashName = Http.hashOrOriginal(origUri, fileName)
    assert(hashName == fileName)
  }

  "A url filename that is shorter than 255 chars and has a path and query params" should "not be changed" in {
    val origUri = Uri.parse("www.bbc.co.uk/news?a=b")
    // Note this not the actual query hash, just representative
    val fileName = "news.74bdbe0d010151d2d42f6768eca1290c"
    val hashName = Http.hashOrOriginal(origUri, fileName)
    assert(hashName == fileName)
  }

  "A url filename that is shorter than 255 chars and has a path, query params and extension" should "not be changed" in {
    val origUri = Uri.parse("www.bbc.co.uk/news.pdf?a=b")
    // Note this not the actual query hash, just representative
    val fileName = "news.74bdbe0d010151d2d42f6768eca1290c.pdf"
    val hashName = Http.hashOrOriginal(origUri, fileName)
    assert(hashName == fileName)
  }

}