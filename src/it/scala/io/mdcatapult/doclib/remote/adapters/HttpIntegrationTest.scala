package io.mdcatapult.doclib.remote.adapters

import java.io.File
import akka.actor.ActorSystem
import akka.stream.Materializer
import better.files.Dsl.pwd
import com.typesafe.config.{Config, ConfigFactory}
import io.lemonlabs.uri.Uri
import io.mdcatapult.doclib.models.Origin
import io.mdcatapult.doclib.remote.DownloadResult
import io.mdcatapult.util.path.DirectoryDeleter.deleteDirectories
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.{BeforeAndAfterAll, Ignore}

import scala.concurrent.Await
import scala.concurrent.duration._

@Ignore
class HttpIntegrationTest extends AnyFlatSpec with BeforeAndAfterAll {

  implicit val config: Config = ConfigFactory.parseString(
    s"""
      |doclib {
      |  root: "${pwd/"test"/"http-test"}"
      |  remote {
      |    target-dir: "remote"
      |    temp-dir: "remote-ingress"
      |  }
      |}
    """.stripMargin)

  private val system = ActorSystem("http-integration-spec")
  implicit val m: Materializer = Materializer(system)

  // TODO URL has moved. Need another long URL.
//  "A uri with calculated filename that is longer than 255 chars with query params" should "be hashed" in {
//    val origUri = Uri.parse("http://orbit.dtu.dk/en/publications/dual-nicotinic-acetylcholine-receptor-42-antagonists7-agonists-synthesis-docking-studies-and-pharmacological-evaluation-of-tetrahydroisoquinolines-and-tetrahydroisoquinolinium-salts(040536e1-22a7-47c7-bee9-51e53543ff04).pdf?nofollow=true&rendering=author")
//    val fileName = "dual-nicotinic-acetylcholine-receptor-42-antagonists7-agonists-synthesis-docking-studies-and-pharmacological-evaluation-of-tetrahydroisoquinolines-and-tetrahydroisoquinolinium-salts(040536e1-22a7-47c7-bee9-51e53543ff04)"
//    val hashedName = Http.md5HashString(fileName)
//    val filePath = Http.generateFilePath(origUri)
//    val queryHash = filePath.replace(".pdf", "").split('.').last
//    val result: Option[DownloadResult] = Http.download(origUri)
//    assert(result.isDefined)
//    val file: ScalaFile = config.getString("doclib.root")/result.get.source
//    assert(file.exists)
//    val sourcePath = Paths.get(s"${config.getString("doclib.remote.temp-dir")}","http", "orbit.dtu.dk", "en", "publications", s"$hashedName.$queryHash.pdf").toString
//    assert(result.get.source == sourcePath)
//  }

  "A valid HTTPS URL" should "download a file successfully" in {
    val origin = Origin("https", uri = Uri.parseOption("https://www.google.com/humans.txt"))
    //val expectedSize = 286
    val result: Option[DownloadResult] = Await.result(Http.download(origin), 10.seconds)
    assert(result.isDefined)
    assert(result.get.isInstanceOf[DownloadResult])
    val file = new File(s"${config.getString("doclib.root")}/${result.get.source}")
    assert(file.exists)
    //assert(file.length == expectedSize)
  }

  "A valid HTTP URL" should "download a file successfully" in {
    val origin = Origin("http", uri = Uri.parseOption("http://www.google.com/robots.txt"))
    //val expectedSize = 7246
    val result: Option[DownloadResult] = Await.result(Http.download(origin), 10.seconds)
    assert(result.isDefined)
    assert(result.get.isInstanceOf[DownloadResult])
    val file = new File(s"${config.getString("doclib.root")}/${result.get.source}")
    assert(file.exists)
    //assert(file.length == expectedSize)
  }

  override def afterAll(): Unit = {
    // These may or may not exist but are all removed anyway
    deleteDirectories(
      List(
        pwd/"test"/"http-test",
        pwd/"test"/"remote-ingress",
        pwd/"test"/"local",
        pwd/"test"/"archive",
        pwd/"test"/"ingress",
        pwd/"test"/"local",
        pwd/"test"/"remote"
      ))
  }
}
