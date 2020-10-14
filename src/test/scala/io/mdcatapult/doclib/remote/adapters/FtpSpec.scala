package io.mdcatapult.doclib.remote.adapters

import akka.actor.ActorSystem
import akka.stream.Materializer
import better.files.Dsl.pwd
import com.typesafe.config.{Config, ConfigFactory}
import io.lemonlabs.uri.Uri
import io.mdcatapult.doclib.models.Origin
import io.mdcatapult.doclib.remote.{UndefinedSchemeException, UnsupportedSchemeException}
import io.mdcatapult.util.path.DirectoryDeleter.deleteDirectories
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

class FtpSpec extends AnyFlatSpec with BeforeAndAfterAll {

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

  private val system = ActorSystem("ftp-spec")
  private implicit val m: Materializer = Materializer(system)

  "A broken FTP URL" should "fail" in {
    val origin = Origin("ftp", uri =  Uri.parseOption("ftp://a.b.c/something"))
    assertThrows[Exception] {
      Ftp.download(origin)
    }
  }

  "A broken SFTP URL" should "fail" in {
    val origin = Origin("ftp", uri = Uri.parseOption("sftp://a.b.c/something"))
    assertThrows[Exception] {
      Ftp.download(origin)
    }
  }
  "A broken FTPS URL" should "fail" in {
    val origin = Origin("ftp", uri = Uri.parseOption("ftps://a.b.c/something"))
    assertThrows[Exception] {
      Ftp.download(origin)
    }
  }

  "An FTP URL with credentials" should "parse" in {
    // The download will fail but we are just testing whether getFTPCredentials works
    val origin = Origin("ftp", uri = Uri.parseOption("ftp://user:password@a.b.c/something"))
    assertThrows[Exception] {
      Ftp.download(origin)
    }
  }

  "An unsupported scheme" should "return exception" in {
    val origin = Origin("ftp", uri = Uri.parseOption("xftp://a.b.c/something"))
    assertThrows[UnsupportedSchemeException] {
      Ftp.download(origin)
    }
  }

  "A URI without a scheme" should "return exception" in {
    val origin = Origin("ftp", uri = Uri.parseOption("a.b.c/something"))
    assertThrows[UndefinedSchemeException] {
      Ftp.download(origin)
    }
  }

  override def afterAll(): Unit = {
    // These may or may not exist but are all removed anyway
    deleteDirectories(Seq(
      pwd / "test" / "remote-ingress",
      pwd / "test" / "remote")
    )
  }
}
