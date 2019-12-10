package io.mdcatapult.doclib.remote.adapters

import java.io.File
import java.nio.file.Paths

import com.typesafe.config.{Config, ConfigFactory}
import io.lemonlabs.uri.Uri
import io.mdcatapult.doclib.remote.DownloadResult
import io.mdcatapult.doclib.util.DirectoryDelete
import org.scalatest.{BeforeAndAfterAll, FlatSpec}
import better.files.{File ⇒ ScalaFile, _}

class HttpIntegrationTest extends FlatSpec with DirectoryDelete with BeforeAndAfterAll {

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

  "A uri with calculated filename that is longer than 255 chars with query params" should "be hashed" in {
    val origUri = Uri.parse("http://orbit.dtu.dk/en/publications/dual-nicotinic-acetylcholine-receptor-42-antagonists7-agonists-synthesis-docking-studies-and-pharmacological-evaluation-of-tetrahydroisoquinolines-and-tetrahydroisoquinolinium-salts(040536e1-22a7-47c7-bee9-51e53543ff04).pdf?nofollow=true&rendering=author")
    val fileName = "dual-nicotinic-acetylcholine-receptor-42-antagonists7-agonists-synthesis-docking-studies-and-pharmacological-evaluation-of-tetrahydroisoquinolines-and-tetrahydroisoquinolinium-salts(040536e1-22a7-47c7-bee9-51e53543ff04)"
    val hashedName = Http.md5HashString(fileName)
    val filePath = Http.generateFilePath(origUri)
    val queryHash = filePath.replace(".pdf", "").split('.').last
    val result: Option[DownloadResult] = Http.download(origUri)
    assert(result.isDefined)
    val file: ScalaFile = config.getString("doclib.root")/result.get.source
    assert(file.exists)
    val sourcePath = Paths.get(s"${config.getString("doclib.remote.temp-dir")}","http", "orbit.dtu.dk", "en", "publications", s"$hashedName.$queryHash.pdf").toString
    assert(result.get.source == sourcePath)
  }

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
}
