package io.mdcatapult.doclib.consumers

import akka.actor.ActorSystem
import akka.stream.Materializer
import com.spingo.op_rabbit.SubscriptionRef
import io.mdcatapult.doclib.consumer.AbstractConsumer
import io.mdcatapult.doclib.handlers.PrefetchHandler
import io.mdcatapult.doclib.messages._
import io.mdcatapult.doclib.models.{DoclibDoc, ParentChildMapping}
import io.mdcatapult.klein.mongo.Mongo
import io.mdcatapult.klein.queue.{Envelope, Queue}
import io.mdcatapult.util.concurrency.SemaphoreLimitedExecution
import io.mdcatapult.util.admin.Server
import org.mongodb.scala.MongoCollection
import play.api.libs.json.Format

object ConsumerPrefetch extends AbstractConsumer() {

  def start()(implicit as: ActorSystem, m: Materializer, mongo: Mongo): SubscriptionRef = {
    import as.dispatcher

    val adminServer = Server(config)

    implicit val collection: MongoCollection[DoclibDoc] =
      mongo.getCollection(config.getString("mongo.doclib-database"), config.getString("mongo.documents-collection"))

    implicit val derivativesCollection: MongoCollection[ParentChildMapping] =
      mongo.getCollection(config.getString("mongo.doclib-database"), config.getString("mongo.derivative-collection"))

    val readLimiter = SemaphoreLimitedExecution.create(config.getInt("mongo.read-limit"))
    val writeLimiter = SemaphoreLimitedExecution.create(config.getInt("mongo.write-limit"))

    // initialise queues
    def queue[T <: Envelope](property: String)(implicit f: Format[T]): Queue[T] =
      Queue[T](config.getString(property),
        consumerName = Some(config.getString("consumer.name")),
        errorQueue = Some(config.getString("doclib.error.queue")))

    val downstream: Queue[DoclibMsg] = queue("doclib.supervisor.queue")
    val upstream: Queue[PrefetchMsg] = queue("consumer.queue")
    val archiver: Queue[DoclibMsg] = queue("doclib.archive.queue")

    adminServer.start()

    upstream.subscribe(
      new PrefetchHandler(downstream, archiver, readLimiter, writeLimiter).handle,
      config.getInt("consumer.concurrency"))
  }
}
