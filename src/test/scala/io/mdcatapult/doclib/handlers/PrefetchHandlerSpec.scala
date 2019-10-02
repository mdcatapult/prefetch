package io.mdcatapult.doclib.handlers

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.testkit.{ImplicitSender, TestKit}
import better.files.Dsl.pwd
import com.mongodb.async.client.{MongoCollection ⇒ JMongoCollection}
import com.typesafe.config.{Config, ConfigFactory}
import io.mdcatapult.doclib.messages.{DoclibMsg, PrefetchMsg}
import io.mdcatapult.doclib.models.metadata.{MetaString, MetaValue}
import io.mdcatapult.doclib.remote.DownloadResult
import io.mdcatapult.doclib.util.MongoCodecs
import io.mdcatapult.klein.queue.Sendable
import org.bson.codecs.configuration.CodecRegistry
import org.mongodb.scala.bson.BsonString
import org.mongodb.scala.{Document, MongoCollection}
import org.scalamock.scalatest.MockFactory
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContextExecutor

/**
 * PrefetchHandler Spec with Actor test system and config
 */
class PrefetchHandlerSpec extends TestKit(ActorSystem("PrefetchHandlerSpec", ConfigFactory.parseString("""
  akka.loggers = ["akka.testkit.TestEventListener"]
  """))) with ImplicitSender
  with WordSpecLike
  with Matchers
  with BeforeAndAfterAll with MockFactory {

  implicit val config: Config = ConfigFactory.parseString(
    """
      |doclib {
      |  root: "/test"
      |  local {
      |    target-dir: "local"
      |    temp-dir: "ingress"
      |  }
      |  remote {
      |    target-dir: "remote"
      |    temp-dir: "remote-ingress"
      |  }
      |  archive {
      |    target-dir: "archive"
      |  }
      |}
    """.stripMargin)

  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val executor: ExecutionContextExecutor = scala.concurrent.ExecutionContext.global
  implicit val mongoCodecs: CodecRegistry = MongoCodecs.get
  val wrappedCollection: JMongoCollection[Document] = stub[JMongoCollection[Document]]
  implicit val collection: MongoCollection[Document] = MongoCollection[Document](wrappedCollection)

  implicit val upstream: Sendable[PrefetchMsg] = stub[Sendable[PrefetchMsg]]
  val downstream: Sendable[DoclibMsg] = stub[Sendable[DoclibMsg]]
  val archiver: Sendable[DoclibMsg] = stub[Sendable[DoclibMsg]]
  val handler = new PrefetchHandler(downstream, archiver)


  "The handler" should {
    "return prefetch message metadata correctly" in {
      val metadataMap: Map[String, Any] = Map[String, Any]("doi" -> "10.1101/327015")
      val prefetchMsg: PrefetchMsg = PrefetchMsg("/a/file/somewhere.pdf", None, Some(List("a-tag")), Some(metadataMap), None)
      val fetchedMetadata = handler.fetchMetaData(prefetchMsg)
      assert(fetchedMetadata.isInstanceOf[List[MetaValue]])
      assert(fetchedMetadata(0).asInstanceOf[MetaString].key == "doi")
      assert(fetchedMetadata(0).asInstanceOf[MetaString].value == "10.1101/327015")
    }

    "correctly identify when a file path is targeting the local root" in {
      assert(handler.inLocalRoot("local/cheese/stinking-bishop.cz"))
    }
    "correctly identify when a file path is not in the local root" in {
      assert(!handler.inLocalRoot("dummy/cheese/stinking-bishop.cz"))
    }

    "correctly identify when a file path is targeting the remote root" in {
      assert(handler.inRemoteRoot("remote/cheese/stinking-bishop.cz"))
    }

    "return an relative local path for local files from a relative ingress path" in {
      val result = handler.getLocalUpdateTargetPath(new handler.FoundDoc(Document(List("source" → BsonString("ingress/cheese/stinking-bishop.cz")))))
      assert(result.get == "local/cheese/stinking-bishop.cz")
    }

    "return an relative local path for local files from a relative local path" in {
      val result = handler.getLocalUpdateTargetPath(new handler.FoundDoc(Document(List("source" → BsonString("local/cheese/stinking-bishop.cz")))))
      assert(result.get == "local/cheese/stinking-bishop.cz")
    }


    "return an relative remote path for remote files from a relative remote ingress path" in {
      val result = handler.getRemoteUpdateTargetPath(new handler.FoundDoc(
        Document(List("source" → BsonString("remote-ingress/cheese/stinking-bishop.cz"))),
        download = Some(DownloadResult("remote-ingress/cheese/stinking-bishop.cz", "1234567890", target = Some("remote/cheese/stinking-bishop.cz")))
      ))
      assert(result.get == "remote/cheese/stinking-bishop.cz")
    }

    "return an relative remote path for remote files from a relative remote path" in {
      val result = handler.getRemoteUpdateTargetPath(new handler.FoundDoc(Document(List("source" → BsonString("remote/cheese/stinking-bishop.cz")))))
      assert(result.get == "remote/cheese/stinking-bishop.cz")
    }

    "return an relative archive path for file from a relative path" in {
      val result = handler.getArchivePath("remote/cheese/stinking-bishop.cz", "fd6eba7e747b846abbdfbfed0e10de12")
      assert(result == "archive/remote/cheese/stinking-bishop.cz/fd6eba7e747b846abbdfbfed0e10de12.cz")
    }

    "return an relative archive path for file from a relative path with no file extension" in {
      val result = handler.getArchivePath("remote/cheese/stinking-bishop", "fd6eba7e747b846abbdfbfed0e10de12")
      assert(result == "archive/remote/cheese/stinking-bishop/fd6eba7e747b846abbdfbfed0e10de12")
    }

    "return an relative doclib path for remote files from a relative remote-ingress path" in {
      val result = handler.getRemoteUpdateTargetPath(new handler.FoundDoc(
        Document(List("source" → BsonString("remote-ingress/cheese/stinking-bishop.cz"))),
        None,
        None,
        Some(DownloadResult("", "", None, Some("remote/cheese/stinking-bishop.cz")))

      ))
      assert(result.get == "remote/cheese/stinking-bishop.cz")
    }
  }

}
