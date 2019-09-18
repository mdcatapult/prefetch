package io.mdcatapult.doclib.handlers

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.testkit.{ImplicitSender, TestKit}
import com.typesafe.config.{Config, ConfigFactory}
import org.mongodb.scala.{ClientSession, Document, MongoCollection}
import org.scalamock.scalatest.MockFactory
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers, WordSpecLike}
import play.api.libs.json.{JsValue, Json}
import com.mongodb.async.client.{MongoCollection => JMongoCollection}
import io.mdcatapult.doclib.messages.PrefetchMsg
import io.mdcatapult.doclib.models.metadata.{MetaString, MetaValue}
import io.mdcatapult.doclib.util.MongoCodecs
import org.bson.codecs.configuration.CodecRegistry
import org.mongodb.scala.bson.BsonString

import scala.concurrent.ExecutionContextExecutor
import scala.collection.JavaConverters._

/**
 * PrefetchHandler Spec with Actor test system and config
 */
class PrefetchHandlerSpec extends TestKit(ActorSystem("PrefetchHandlerSpec", ConfigFactory.parseString("""
  akka.loggers = ["akka.testkit.TestEventListener"]
  """))) with ImplicitSender
  with WordSpecLike
  with Matchers
  with BeforeAndAfterAll with MockFactory {

  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val executor: ExecutionContextExecutor = scala.concurrent.ExecutionContext.global
  implicit val config: Config = ConfigFactory.parseMap(Map[String, Any]().asJava)
  val wrapped = mock[JMongoCollection[Document]]
  implicit val collection = MongoCollection[Document](wrapped)
  implicit val mongoCodecs: CodecRegistry = MongoCodecs.get

  "The handler" should {
    "return prefetch message metadata correctly" in {
      val prefetchHandler: PrefetchHandler = new PrefetchHandler(null, null)
      val metadataMap: Map[String, Any] = Map[String, Any]("doi" -> "10.1101/327015")
      val prefetchMsg: PrefetchMsg = PrefetchMsg("/a/file/somewhere.pdf", None, Some(List("a-tag")), Some(metadataMap), None)
      val fetchedMetadata = prefetchHandler.fetchMetaData(prefetchMsg)
      assert(fetchedMetadata.isInstanceOf[List[MetaValue]])
      assert(fetchedMetadata(0).asInstanceOf[MetaString].key == "doi")
      assert(fetchedMetadata(0).asInstanceOf[MetaString].value == "10.1101/327015")
    }
  }
}
