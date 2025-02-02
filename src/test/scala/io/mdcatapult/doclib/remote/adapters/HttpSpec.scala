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

package io.mdcatapult.doclib.remote.adapters

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.{Materializer, StreamTcpException}
import better.files.Dsl.pwd
import com.typesafe.config.{Config, ConfigFactory}
import io.lemonlabs.uri.Uri
import io.mdcatapult.doclib.models.Origin
import io.mdcatapult.util.path.DirectoryDeleter.deleteDirectories
import org.scalatest.BeforeAndAfterAll
import org.scalatest.RecoverMethods.{recoverToExceptionIf, recoverToSucceededIf}
import org.scalatest.flatspec.AnyFlatSpec

import scala.concurrent.ExecutionContext.Implicits.global

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
    recoverToSucceededIf[StreamTcpException] {
      Http.download(origin)
    }
  }

  "A valid URL with unknown file" should "throw an Exception" in {
    val source = "http://www.google.com/this-is-an-invalid-file.pdf"
    val origin = Origin("http", uri = Uri.parseOption(source))
    val caught = recoverToExceptionIf[Exception] {
      Http.download(origin)
    }
    caught.map { ex => assert(ex.getMessage == s"Unable to process $source with status code 404 Not Found")}
  }

  "Http protocols" should "be appropriate" in {
    Http.protocols.equals(List("http", "https"))
  }

  override def afterAll(): Unit = {
    // These may or may not exist but are all removed anyway
    deleteDirectories(Seq(
      pwd/"test"/"remote-ingress",
      pwd/"test"/"remote")
    )
  }

}
