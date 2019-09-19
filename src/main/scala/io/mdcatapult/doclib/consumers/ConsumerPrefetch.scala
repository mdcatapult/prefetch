package io.mdcatapult.doclib.consumers

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.spingo.op_rabbit.SubscriptionRef
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.LazyLogging
import io.mdcatapult.doclib.handlers.PrefetchHandler
import io.mdcatapult.doclib.messages._
import io.mdcatapult.doclib.models.metadata.MetaString
import io.mdcatapult.doclib.models.{DoclibFlag, FileAttrs}
import io.mdcatapult.klein.mongo.Mongo
import io.mdcatapult.klein.queue.Queue
import org.bson.codecs.configuration.CodecRegistries.{fromProviders, fromRegistries}
import org.bson.codecs.configuration.CodecRegistry
import org.mongodb.scala.bson.codecs.DEFAULT_CODEC_REGISTRY
import org.mongodb.scala.bson.codecs.Macros._
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
  implicit val mongoCodecs: CodecRegistry = fromRegistries(fromProviders(classOf[PrefetchMsg], classOf[FileAttrs], classOf[MetaString], classOf[DoclibFlag]), DEFAULT_CODEC_REGISTRY )
  implicit val mongo: Mongo = new Mongo()
  implicit val collection: MongoCollection[Document] = mongo.collection
  val archiveCollection = mongo.getCollection(Some(config.getString("mongo.archive-collection")))

  /** initialise queues **/
  val downstream: Queue[DoclibMsg] = new Queue[DoclibMsg](config.getString("downstream.queue"))
  val upstream: Queue[PrefetchMsg] = new Queue[PrefetchMsg](config.getString("upstream.queue"))
  val subscription: SubscriptionRef = upstream.subscribe(new PrefetchHandler(downstream, archiveCollection).handle, config.getInt("upstream.concurrent"))

}
