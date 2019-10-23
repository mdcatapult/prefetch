package io.mdcatapult.doclib.remote.adapters

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
    val result: Option[DownloadResult] = Http.download(origUri)
    assert(result.isDefined)
    val file: ScalaFile = config.getString("doclib.root")/result.get.source
    assert(file.exists)
  }
}
