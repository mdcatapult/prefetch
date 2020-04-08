package io.mdcatapult.doclib.remote.adapters

import akka.actor.ActorSystem
import akka.stream.Materializer
import better.files.Dsl.pwd
import com.typesafe.config.{Config, ConfigFactory}
import io.lemonlabs.uri.Uri
import io.mdcatapult.doclib.remote.{UndefinedSchemeException, UnsupportedSchemeException}
import io.mdcatapult.doclib.util.DirectoryDelete
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

class FtpSpec extends AnyFlatSpec with BeforeAndAfterAll with DirectoryDelete {

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
    val uri = Uri.parse("ftp://a.b.c/something")
    assertThrows[Exception] {
      Ftp.download(uri)
    }
  }

  "A broken SFTP URL" should "fail" in {
    val uri = Uri.parse("sftp://a.b.c/something")
    assertThrows[Exception] {
      Ftp.download(uri)
    }
  }
  "A broken FTPS URL" should "fail" in {
    val uri = Uri.parse("ftps://a.b.c/something")
    assertThrows[Exception] {
      Ftp.download(uri)
    }
  }

  "An FTP URL with credentials" should "parse" in {
    // The download will fail but we are just testing whether getFTPCredentials works
    val uri = Uri.parse("ftp://user:password@a.b.c/something")
    assertThrows[Exception] {
      Ftp.download(uri)
    }
  }

  "An unsupported scheme" should "return exception" in {
    val uri = Uri.parse("xftp://a.b.c/something")
    assertThrows[UnsupportedSchemeException] {
      Ftp.download(uri)
    }
  }

  "A URI without a scheme" should "return exception" in {
    val uri = Uri.parse("a.b.c/something")
    assertThrows[UndefinedSchemeException] {
      Ftp.download(uri)
    }
  }
  override def afterAll: Unit = {
    // These may or may not exist but are all removed anyway
    deleteDirectories(List(
      pwd / "test" / "remote-ingress",
      pwd / "test" / "remote")
    )
  }

}
