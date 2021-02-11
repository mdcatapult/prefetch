package io.mdcatapult.doclib.remote.adapters

import akka.actor.ActorSystem
import akka.stream.{Materializer}
import better.files.Dsl.pwd
import com.typesafe.config.{Config, ConfigFactory}
import io.lemonlabs.uri.Uri
import io.mdcatapult.doclib.models.Origin
import io.mdcatapult.util.path.DirectoryDeleter.deleteDirectories
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

class HttpSpec extends AnyFlatSpec with BeforeAndAfterAll {

  implicit val config: Config = ConfigFactory.parseString(
    s"""
      |doclib {
      |  root: "$pwd/test"
      |  remote {
      |    target-dir: "remote"
      |    temp-dir: "remote-ingress"
      |  }
      |}
    """.stripMargin)

  private val system = ActorSystem("http-spec")
  private implicit val m: Materializer = Materializer(system)

  "An URL to nowhere" should "throw an Exception" in {
    val origin = Origin("http", uri = Uri.parseOption("http://www.a.b.c/something"))
    val v = Http.download(origin)
    println(v)
//    assertThrows[StreamTcpException] {
//
//    }
  }

  "A valid URL with unknown file" should "throw an Exception" in {
    val source = "http://www.google.com/this-is-an-invalid-file.pdf"
    val origin = Origin("http", uri = Uri.parseOption(source))
    val caught = intercept[Exception] {
      Http.download(origin)
    }
    assert(caught.getMessage == s"Unable to process $source with status code 404 Not Found")
  }

  override def afterAll(): Unit = {
    // These may or may not exist but are all removed anyway
    deleteDirectories(Seq(
      pwd/"test"/"remote-ingress",
      pwd/"test"/"remote")
    )
  }

}
