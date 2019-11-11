package io.mdcatapult.doclib.consumers

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.spingo.op_rabbit.SubscriptionRef
import io.mdcatapult.doclib.consumer.AbstractConsumer
import io.mdcatapult.doclib.handlers.PrefetchHandler
import io.mdcatapult.doclib.messages._
import io.mdcatapult.doclib.models.DoclibDoc
import io.mdcatapult.klein.mongo.Mongo
import io.mdcatapult.klein.queue.Queue
import org.mongodb.scala.MongoCollection

import scala.concurrent.ExecutionContextExecutor

object ConsumerPrefetch extends AbstractConsumer("consumer-prefetch") {

  def start()(implicit as: ActorSystem, materializer: ActorMaterializer, mongo: Mongo): SubscriptionRef = {
    implicit val ex: ExecutionContextExecutor = as.dispatcher
    implicit val collection: MongoCollection[DoclibDoc] = mongo.database.getCollection(config.getString("mongo.collection"))

    /** initialise queues **/
    val downstream: Queue[DoclibMsg] = new Queue[DoclibMsg](config.getString("doclib.supervisor.queue"), consumerName = Some("prefetch"))
    val upstream: Queue[PrefetchMsg] = new Queue[PrefetchMsg](config.getString("upstream.queue"), consumerName = Some("prefetch"))
    val archiver: Queue[DoclibMsg] = new Queue[DoclibMsg](config.getString("doclib.archive.queue"), consumerName = Some("prefetch"))
    upstream.subscribe(new PrefetchHandler(downstream, archiver).handle, config.getInt("upstream.concurrent"))
  }
}
