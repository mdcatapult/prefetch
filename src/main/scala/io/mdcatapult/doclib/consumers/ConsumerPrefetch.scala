package io.mdcatapult.doclib.consumers

import akka.actor.ActorSystem
import akka.stream.Materializer
import com.spingo.op_rabbit.SubscriptionRef
import io.mdcatapult.doclib.consumer.AbstractConsumer
import io.mdcatapult.doclib.handlers.{AdminServer, PrefetchHandler}
import io.mdcatapult.doclib.messages._
import io.mdcatapult.doclib.models.{DoclibDoc, ParentChildMapping}
import io.mdcatapult.klein.mongo.Mongo
import io.mdcatapult.klein.queue.{Envelope, Queue}
import io.mdcatapult.util.concurrency.SemaphoreLimitedExecution
import io.prometheus.client.hotspot.DefaultExports
import org.mongodb.scala.MongoCollection
import play.api.libs.json.Format

object ConsumerPrefetch extends AbstractConsumer("consumer-prefetch") {

  def start()(implicit as: ActorSystem, m: Materializer, mongo: Mongo): SubscriptionRef = {
    import as.dispatcher

    DefaultExports.initialize()
    val adminServer = AdminServer(config)

    implicit val collection: MongoCollection[DoclibDoc] =
      mongo.database.getCollection(config.getString("mongo.collection"))

    implicit val derivativesCollection: MongoCollection[ParentChildMapping] =
      mongo.database.getCollection(config.getString("mongo.derivative-collection"))

    val readLimiter = SemaphoreLimitedExecution.create(config.getInt("mongo.limit.read"))
    val writeLimiter = SemaphoreLimitedExecution.create(config.getInt("mongo.limit.write"))

    // initialise queues
    def queue[T <: Envelope](property: String)(implicit f: Format[T]): Queue[T] =
      Queue[T](config.getString(property), consumerName = Some("prefetch"))

    val downstream: Queue[DoclibMsg] = queue("doclib.supervisor.queue")
    val upstream: Queue[PrefetchMsg] = queue("upstream.queue")
    val archiver: Queue[DoclibMsg] = queue("doclib.archive.queue")

    adminServer.start()

    upstream.subscribe(
      new PrefetchHandler(downstream, archiver, readLimiter, writeLimiter).handle,
      config.getInt("upstream.concurrent"))
  }
}
