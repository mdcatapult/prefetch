package io.mdcatapult.doclib.consumers

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.spingo.op_rabbit.SubscriptionRef
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.LazyLogging
import io.mdcatapult.doclib.handlers.PrefetchHandler
import io.mdcatapult.doclib.messages._
import io.mdcatapult.doclib.models._
import io.mdcatapult.doclib.util._
import io.mdcatapult.klein.mongo.Mongo
import io.mdcatapult.klein.queue.{Exchange, Queue}
import org.bson.codecs.configuration.CodecRegistries.{fromProviders, fromRegistries}
import org.bson.codecs.configuration.{CodecRegistries, CodecRegistry}
import org.bson.codecs.jsr310.LocalDateTimeCodec
import org.mongodb.scala.{Document, MongoCollection}
import org.mongodb.scala.bson.codecs.DEFAULT_CODEC_REGISTRY
import org.mongodb.scala.bson.codecs.Macros._

import scala.concurrent.ExecutionContextExecutor

object ConsumerPrefetch extends App with LazyLogging {

  /** initialise implicit dependencies **/
  implicit val system: ActorSystem = ActorSystem("consumer-prefetch")
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val executor: ExecutionContextExecutor = scala.concurrent.ExecutionContext.global
  implicit val config: Config = ConfigFactory.load()

  /** Initialise Mongo **/
  implicit val mongoCodecs: CodecRegistry = fromRegistries(fromProviders(
    classOf[PrefetchOrigin],
    classOf[FileAttrs]),
    CodecRegistries.fromCodecs(
      new LocalDateTimeCodec,
      new LemonLabsAbsoluteUrlCodec,
      new LemonLabsRelativeUrlCodec,
      new LemonLabsUrlCodec),
    DEFAULT_CODEC_REGISTRY)
  implicit val mongo: Mongo = new Mongo()
  implicit val collection: MongoCollection[Document] = mongo.collection

  /** initialise queues **/
  val downstream: Queue[DoclibMsg] = new Queue[DoclibMsg](config.getString("downstream.queue"))
  val upstream: Queue[PrefetchMsg] = new Queue[PrefetchMsg](config.getString("upstream.queue"))
  val subscription: SubscriptionRef = upstream.subscribe(new PrefetchHandler(downstream).handle, config.getInt("upstream.concurrent"))




}
