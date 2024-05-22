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

package io.mdcatapult.doclib.consumers

import akka.actor.ActorSystem
import akka.stream.Materializer
import io.mdcatapult.doclib.consumer.AbstractConsumer
import io.mdcatapult.doclib.handlers.{PrefetchHandler, PrefetchResult, SupervisorHandlerResult}
import io.mdcatapult.doclib.messages._
import io.mdcatapult.doclib.models.{AppConfig, DoclibDoc, ParentChildMapping}
import io.mdcatapult.klein.mongo.Mongo
import io.mdcatapult.klein.queue.Queue
import io.mdcatapult.util.concurrency.SemaphoreLimitedExecution
import io.mdcatapult.util.admin.Server
import org.mongodb.scala.MongoCollection

import scala.util.Try

object ConsumerPrefetch extends AbstractConsumer[PrefetchMsg, PrefetchResult]() {

  def start()(implicit as: ActorSystem, m: Materializer, mongo: Mongo): Unit = {

    import as.dispatcher

    val adminServer = Server(config)

    implicit val appConfig: AppConfig =
      AppConfig(
        config.getString("consumer.name"),
        config.getInt("consumer.concurrency"),
        config.getString("consumer.queue"),
        Try(config.getString("consumer.exchange")).toOption
      )

    implicit val collection: MongoCollection[DoclibDoc] =
      mongo.getCollection(config.getString("mongo.doclib-database"), config.getString("mongo.documents-collection"))

    implicit val derivativesCollection: MongoCollection[ParentChildMapping] =
      mongo.getCollection(config.getString("mongo.doclib-database"), config.getString("mongo.derivative-collection"))

    val readLimiter: SemaphoreLimitedExecution = SemaphoreLimitedExecution.create(config.getInt("mongo.read-limit"))
    val writeLimiter = SemaphoreLimitedExecution.create(config.getInt("mongo.write-limit"))

    // Note that the SupervisorHandlerResult is because the Queue expects the type of response from the "business logic"
    // In reality we don't care here because we are just sending and not subscribing.
    val downstream: Queue[SupervisorMsg, SupervisorHandlerResult] = Queue[SupervisorMsg, SupervisorHandlerResult](config.getString("doclib.supervisor.queue"))
    // 'queue' is just a convenience method from older versions which didn't use Alpakka AMQP.
    // TODO Remove 'queue' method to avoid confusion
    val upstream: Queue[PrefetchMsg, PrefetchResult] = queue("consumer.queue")

    adminServer.start()

    upstream.subscribe(
      new PrefetchHandler(downstream, readLimiter, writeLimiter).handle,
      config.getInt("consumer.concurrency"))
  }
}
