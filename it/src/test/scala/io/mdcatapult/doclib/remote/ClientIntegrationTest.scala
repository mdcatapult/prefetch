/*
 * Copyright 2024 Medicines Discovery Catapult
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.mdcatapult.doclib.remote

import java.nio.file.{Files, Paths}

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.{Materializer, StreamTcpException}
import org.apache.pekko.testkit.{ImplicitSender, TestKit}
import better.files.Dsl._
import com.typesafe.config.{Config, ConfigFactory}
import io.lemonlabs.uri.Uri
import io.mdcatapult.doclib.models.Origin
import io.mdcatapult.util.path.DirectoryDeleter.deleteDirectories
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpecLike
import org.scalatest.{Assertion, BeforeAndAfterAll, OptionValues}
import scala.concurrent.duration._
import scala.concurrent.Await
import scala.io.Source

class ClientIntegrationTest  extends TestKit(ActorSystem("ClientIntegrationTest", ConfigFactory.parseString(
  """
  akka.loggers = ["akka.testkit.TestEventListener"]
  """))) with ImplicitSender
  with AsyncWordSpecLike
  with Matchers
  with BeforeAndAfterAll with OptionValues {
  implicit val config: Config = ConfigFactory.parseString(
    """
      |doclib {
      |  root: "./test"
      |  remote {
      |    target-dir: "remote"
      |    temp-dir: "remote-ingress"
      |  }
      |  local {
      |    target-dir: "local"
      |    temp-dir: "ingress"
      |  }
      |  archive {
      |    target-dir: "archive"
      |  }
      |}
    """.stripMargin)

  implicit val m: Materializer = Materializer(system)
  val client = new Client()

  private def verifySourceIsPdf(source: String): Assertion = {
    val docMagicWord = Array.ofDim[Char](4)
    Source.fromFile("test/" + source, "UTF-8").reader().read(docMagicWord)

    docMagicWord should be (Array('%', 'P', 'D', 'F'))
  }

  "A valid https URI " should {
    "be downloadable" ignore {
      val origin: Origin = Origin("https", uri = Uri.parseOption("https://www.bbc.co.uk/news"))
      val a = Await.result(client.download(origin), 10.seconds)
      assert(a.value.origin.value == "https://www.bbc.co.uk/news")
      assert(a.value.target.value == s"$pwd/${config.getString("doclib.root")}/${config.getString("doclib.remote.target-dir")}/https/www.bbc.co.uk/news.html")
      assert(a.value.source == s"${config.getString("doclib.remote.temp-dir")}/https/www.bbc.co.uk/news.html")
    }
  }

  // These resources are not reliable
  "A valid http URI that relies on cookie " should {
    "be downloadable" ignore  {
      val origin: Origin = Origin("http", uri = Uri.parseOption("http://www.tandfonline.com/doi/pdf/10.4081/ijas.2015.3712?needAccess=true"))
      val a = Await.result(client.download(origin), 10.seconds)

      assert(a.value.origin.value == "http://www.tandfonline.com/doi/pdf/10.4081/ijas.2015.3712?needAccess=true")
      assert(a.value.target.value == s"$pwd/${config.getString("doclib.root")}/${config.getString("doclib.remote.target-dir")}/http/www.tandfonline.com/doi/pdf/10.4081/ijas.2015.3712/Effects of Verbascoside Supplemented Diets on Growth Performance Blood Traits Meat Quality Lipid Oxidation and Histological Features in Broiler.f3e24e247796d0e8aadc4607bfdfdbe4.pdf")
      assert(a.value.source == s"${config.getString("doclib.remote.temp-dir")}/http/www.tandfonline.com/doi/pdf/10.4081/ijas.2015.3712/Effects of Verbascoside Supplemented Diets on Growth Performance Blood Traits Meat Quality Lipid Oxidation and Histological Features in Broiler.f3e24e247796d0e8aadc4607bfdfdbe4.pdf")

      verifySourceIsPdf(a.value.source)
    }
  }

  "A valid https URI that relies on cookie " should {
    "be downloadable" ignore {
      val origin: Origin = Origin("https", uri = Uri.parseOption("https://www.tandfonline.com/doi/pdf/10.4081/ijas.2015.3712?needAccess=true"))
      val a = Await.result(client.download(origin), 10.seconds)
      assert(a.value.origin.value == "https://www.tandfonline.com/doi/pdf/10.4081/ijas.2015.3712?needAccess=true")
      assert(a.value.target.value == s"$pwd/${config.getString("doclib.root")}/${config.getString("doclib.remote.target-dir")}/https/www.tandfonline.com/doi/pdf/10.4081/ijas.2015.3712/Effects of Verbascoside Supplemented Diets on Growth Performance Blood Traits Meat Quality Lipid Oxidation and Histological Features in Broiler.f3e24e247796d0e8aadc4607bfdfdbe4.pdf")
      assert(a.value.source == s"${config.getString("doclib.remote.temp-dir")}/https/www.tandfonline.com/doi/pdf/10.4081/ijas.2015.3712/Effects of Verbascoside Supplemented Diets on Growth Performance Blood Traits Meat Quality Lipid Oxidation and Histological Features in Broiler.f3e24e247796d0e8aadc4607bfdfdbe4.pdf")

      verifySourceIsPdf(a.value.source)
    }
  }

  "An invalid https URI " should {
    "throw an exception" in {
      val origin: Origin = Origin("https", uri = Uri.parseOption("https://a.b.c"))
      recoverToSucceededIf[StreamTcpException] {
        client.download(origin)
      }
    }
  }

  "A valid FTP URI " should {
    "be downloadable" ignore {
      val origin: Origin = Origin("ftp", uri = Uri.parseOption("ftp://ftp.ebi.ac.uk/pub/databases/pmc/suppl/PRIVACY-NOTICE.txt"))
      val a = Await.result(client.download(origin), 10.seconds)
      assert(a.value.origin.value == "ftp://ftp.ebi.ac.uk/pub/databases/pmc/suppl/PRIVACY-NOTICE.txt")
      assert(a.value.target.value == s"$pwd/${config.getString("doclib.root")}/${config.getString("doclib.remote.target-dir")}/ftp/ftp.ebi.ac.uk/pub/databases/pmc/suppl/PRIVACY-NOTICE.txt")
      assert(a.value.source == s"${config.getString("doclib.remote.temp-dir")}/ftp/ftp.ebi.ac.uk/pub/databases/pmc/suppl/PRIVACY-NOTICE.txt")
      assert(Files.exists(Paths.get(s"$pwd/${config.getString("doclib.root")}/${config.getString("doclib.remote.temp-dir")}/ftp/ftp.ebi.ac.uk/pub/databases/pmc/suppl/PRIVACY-NOTICE.txt")))
    }
  }

  override def afterAll(): Unit = {
    // These may or may not exist but are all removed anyway
    deleteDirectories(
      List("remote-ingress", "local", "archive", "ingress", "local", "remote").map(pwd/"test"/_)
    )
  }
}
