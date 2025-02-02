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

import java.net.URL

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.Materializer
import com.typesafe.config.{Config, ConfigFactory}
import io.lemonlabs.uri._
import io.mdcatapult.doclib.models.Origin
import org.scalatest.flatspec.AnyFlatSpec

import scala.concurrent.Await
import scala.concurrent.duration.Duration

class ClientSpec extends AnyFlatSpec {

  val wsConfFile: URL = getClass.getResource("/test/ws.conf")
  val wsConfig: Config = ConfigFactory.parseURL(wsConfFile)
  implicit val config: Config = ConfigFactory.parseString(
    """
      |doclib {
      |  remote {
      |    target-dir: "./test"
      |    temp-dir: "./test"
      |  }
      |}
      |play {
      |  ws {
      |    ahc {
      |        connectionPoolCleanerPeriod: 1 second
      |    }
      |  }
      |}
    """.stripMargin)
    .withFallback(wsConfig)
  private val system: ActorSystem = ActorSystem("scalatest", config)
  private implicit val m: Materializer = Materializer(system)

  import system.dispatcher
  private val client = new Client()

  "A valid http URL that redirects to https" should "resolve to a valid Prefetch Origin" in {
    val result = Await.result(client.resolve(Uri.parse("http://news.bbc.co.uk")), Duration.Inf)
    assert(result.head.scheme == "https")
    assert(result.head.uri.get.schemeOption.get == "https")
    assert(result.head.uri.get.toUrl.hostOption.get.toString == "www.bbc.co.uk")
    assert(result.head.uri.get.toUrl.path.toAbsolute.toString == "/news")
    assert(result.head.hostname.get == "www.bbc.co.uk")
  }

  "A valid ftp URL" should "resolve to a valid Prefetch Origin" in {
    val source = Uri.parse("ftp://ftp.ebi.ac.uk/pub/databases/pmc/suppl/PRIVACY-NOTICE.txt")
    val result = Await.result(client.resolve(source), Duration.Inf)
    assert(result.head.scheme == "ftp")
    assert(result.head.uri.get == source)
    assert(result.head.hostname.get == "ftp.ebi.ac.uk")
  }

  "An unsupported scheme" should "throw an exception" in {
    val source = Uri.parse("file://a_file.txt")
    assertThrows[UnsupportedSchemeException] {
      client.resolve(source)
    }
  }

  "An undefined scheme" should "throw an exception" in {
    val source = Uri.parse("a_file.txt")
    assertThrows[UndefinedSchemeException] {
      client.resolve(source)
    }
  }
  "Downloading an unsupported scheme" should "throw an exception" in {
    val origin = Origin("file", uri = Uri.parseOption("file://a_file.txt"))
    assertThrows[UnsupportedSchemeException] {
      client.download(origin)
    }
  }
}
