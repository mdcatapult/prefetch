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

package io.mdcatapult.doclib.handlers

import better.files.Dsl.pwd
import com.typesafe.config.{Config, ConfigFactory}
import io.mdcatapult.doclib.codec.MongoCodecs
import io.mdcatapult.doclib.models.{AppConfig, DoclibDoc, ParentChildMapping}
import io.mdcatapult.klein.mongo.Mongo
import io.mdcatapult.util.concurrency.SemaphoreLimitedExecution
import io.mdcatapult.util.path.DirectoryDeleter.deleteDirectories
import org.bson.codecs.configuration.CodecRegistry
import org.mongodb.scala.MongoCollection
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpecLike

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.DurationInt

trait PrefetchHandlerBaseTest extends AnyFlatSpecLike with BeforeAndAfterAll {

  implicit val config: Config = ConfigFactory.parseString(
    s"""
       |doclib {
       |  root: "$pwd/test/prefetch-test"
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
       |  derivative {
       |    target-dir: "derivatives"
       |  }
       |}
       |consumer {
       |  name = "prefetch"
       |  queue = "prefetch"
       |  concurrency = 1
       |  exchange = "doclib"
       |}
       |mongo {
       |  doclib-database: "prefetch-test"
       |  documents-collection: "documents"
       |  derivatives-collection : "derivatives"
       |}
    """.stripMargin).withFallback(ConfigFactory.load())

  implicit val appConfig: AppConfig =
    AppConfig(
      config.getString("consumer.name"),
      config.getInt("consumer.concurrency"),
      config.getString("consumer.queue"),
      Option(config.getString("consumer.exchange"))
    )

  /** Initialise Mongo * */
  implicit val codecs: CodecRegistry = MongoCodecs.get
  val mongo: Mongo = new Mongo()

  implicit val collection: MongoCollection[DoclibDoc] = mongo.getCollection(config.getString("mongo.doclib-database"), config.getString("mongo.documents-collection"))
  implicit val derivativesCollection: MongoCollection[ParentChildMapping] = mongo.getCollection(config.getString("mongo.doclib-database"), config.getString("mongo.derivatives-collection"))




  val readLimiter: SemaphoreLimitedExecution = SemaphoreLimitedExecution.create(config.getInt("mongo.read-limit"))
  val writeLimiter: SemaphoreLimitedExecution = SemaphoreLimitedExecution.create(config.getInt("mongo.write-limit"))

  override def afterAll(): Unit = {
    Await.result(collection.drop().toFutureOption(), 5.seconds)
    Await.result(derivativesCollection.drop().toFutureOption(), 5.seconds)
    // These may or may not exist but are all removed anyway
    deleteDirectories(List(pwd / "test" / "prefetch-test"))
  }

}