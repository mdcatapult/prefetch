package io.mdcatapult.doclib.consumers

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.spingo.op_rabbit.SubscriptionRef
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.LazyLogging
import io.mdcatapult.doclib.handlers.PrefetchHandler
import io.mdcatapult.doclib.messages._
import io.mdcatapult.doclib.util.MongoCodecs
import io.mdcatapult.klein.mongo.Mongo
import io.mdcatapult.klein.queue.Queue
import org.bson.codecs.configuration.CodecRegistry
import org.mongodb.scala.{Document, MongoCollection}

import scala.concurrent.ExecutionContextExecutor

object ConsumerPrefetch extends App with LazyLogging {

  /** initialise implicit dependencies **/
  implicit val system: ActorSystem = ActorSystem("consumer-prefetch")
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val executor: ExecutionContextExecutor = scala.concurrent.ExecutionContext.global
  implicit val config: Config = ConfigFactory.load()

  /** Initialise Mongo **/
  // Custom mongo codecs for saving message
  implicit val codecs: CodecRegistry = MongoCodecs.get
  implicit val mongo: Mongo = new Mongo()
  implicit val collection: MongoCollection[Document] = mongo.collection
  val archiveCollection = mongo.getCollection(Some(config.getString("mongo.archive-collection")))

  /** initialise queues **/
  val downstream: Queue[DoclibMsg] = new Queue[DoclibMsg](config.getString("downstream.queue"), Option(config.getString("op-rabbit.topic-exchange-name")))
  val upstream: Queue[PrefetchMsg] = new Queue[PrefetchMsg](config.getString("upstream.queue"), Option(config.getString("op-rabbit.topic-exchange-name")))
  val subscription: SubscriptionRef = upstream.subscribe(new PrefetchHandler(downstream, archiveCollection).handle, config.getInt("upstream.concurrent"))

}
