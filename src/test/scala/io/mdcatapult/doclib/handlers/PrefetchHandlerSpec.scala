package io.mdcatapult.doclib.handlers

import java.time.{LocalDateTime, ZoneOffset}

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.testkit.{ImplicitSender, TestKit}
import better.files.{File ⇒ ScalaFile}
import com.mongodb.async.client.{MongoCollection ⇒ JMongoCollection}
import com.typesafe.config.{Config, ConfigFactory}
import io.lemonlabs.uri.Uri
import io.mdcatapult.doclib.messages.{DoclibMsg, PrefetchMsg}
import io.mdcatapult.doclib.models.metadata.MetaString
import io.mdcatapult.doclib.models.{DoclibDoc, FileAttrs, Origin}
import io.mdcatapult.doclib.remote.DownloadResult
import io.mdcatapult.doclib.util.MongoCodecs
import io.mdcatapult.klein.queue.Sendable
import org.bson.codecs.configuration.CodecRegistry
import org.mongodb.scala.MongoCollection
import org.mongodb.scala.bson.{BsonInt32, ObjectId}
import org.scalamock.scalatest.MockFactory
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContextExecutor}

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
      |  derivative {
      |    target-dir: "derivatives"
      |  }
      |}
    """.stripMargin)

  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val executor: ExecutionContextExecutor = scala.concurrent.ExecutionContext.global
  implicit val mongoCodecs: CodecRegistry = MongoCodecs.get
  val wrappedCollection: JMongoCollection[DoclibDoc] = stub[JMongoCollection[DoclibDoc]]
  implicit val collection: MongoCollection[DoclibDoc] = MongoCollection[DoclibDoc](wrappedCollection)

  implicit val upstream: Sendable[PrefetchMsg] = stub[Sendable[PrefetchMsg]]
  val downstream: Sendable[DoclibMsg] = stub[Sendable[DoclibMsg]]
  val archiver: Sendable[DoclibMsg] = stub[Sendable[DoclibMsg]]
  val handler = new PrefetchHandler(downstream, archiver)

  def createNewDoc(source: String): DoclibDoc = {
    val createdTime = LocalDateTime.now().toInstant(ZoneOffset.UTC)
    val path = ScalaFile(source).path
    val fileAttrs = FileAttrs(
      path = path.getParent.toAbsolutePath.toString,
      name = path.getFileName.toString,
      mtime = LocalDateTime.ofInstant(createdTime, ZoneOffset.UTC),
      ctime = LocalDateTime.ofInstant(createdTime, ZoneOffset.UTC),
      atime = LocalDateTime.ofInstant(createdTime, ZoneOffset.UTC),
      size = 5
    )
    //TODO what should created and updated time be. Mime type? From getMimeType or from some metadata? More than one?
    val newDoc = DoclibDoc(
      _id = new ObjectId(),
      source = source,
      hash = "12345",
      derivative = false,
      created = LocalDateTime.ofInstant(createdTime, ZoneOffset.UTC),
      updated = LocalDateTime.ofInstant(createdTime, ZoneOffset.UTC),
      mimetype = "",
      attrs = Some(fileAttrs)
    )
    newDoc
  }


  "The handler" should {
    "return prefetch message metadata correctly" in {
      val metadataMap: List[MetaString] = List(MetaString("doi", "10.1101/327015"))
      val prefetchMsg: PrefetchMsg = PrefetchMsg("/a/file/somewhere.pdf", None, Some(List("a-tag")), Some(metadataMap), None)
      val fetchedMetadata = prefetchMsg.metadata
      assert(fetchedMetadata.get.length == 1)
      assert(fetchedMetadata.get.head.getKey == "doi")
      assert(fetchedMetadata.get.head.getValue == "10.1101/327015")
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
      val result = handler.getLocalUpdateTargetPath(handler.FoundDoc(createNewDoc("ingress/cheese/stinking-bishop.cz")))
      assert(result.get == "local/cheese/stinking-bishop.cz")
    }

    "return an relative local path for local files from a relative local path" in {
      val result = handler.getLocalUpdateTargetPath(handler.FoundDoc(createNewDoc("local/cheese/stinking-bishop.cz")))
      assert(result.get == "local/cheese/stinking-bishop.cz")
    }


    "return an relative remote path for remote files from a relative remote ingress path" in {
      val result = handler.getRemoteUpdateTargetPath(handler.FoundDoc(doc =
        createNewDoc("remote-ingress/cheese/stinking-bishop.cz"),
        download = Some(DownloadResult("remote-ingress/cheese/stinking-bishop.cz", "1234567890", target = Some("remote/cheese/stinking-bishop.cz")))
      ))
      assert(result.get == "remote/cheese/stinking-bishop.cz")
    }

    "return an relative remote path for remote files from a relative remote path" in {
      val result = handler.getRemoteUpdateTargetPath(handler.FoundDoc(createNewDoc("remote/cheese/stinking-bishop.cz")))
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
      val result = handler.getRemoteUpdateTargetPath(handler.FoundDoc(
        doc = createNewDoc("remote-ingress/cheese/stinking-bishop.cz"),
        None,
        None,
        Some(DownloadResult("", "", None, Some("remote/cheese/stinking-bishop.cz")))

      ))
      assert(result.get == "remote/cheese/stinking-bishop.cz")
    }

    "return a relative doclib path for local files with an FTP remote origin" in {
      val origin: Origin = Origin(
        scheme = "ftp",
        hostname = None,
        uri = Some(Uri.parse("ftp://a.site/a/path/to/aFile.txt")),
        metadata = None,
        headers = None
      )
      val foundDoc = handler.FoundDoc(
        doc = createNewDoc("ingress/ebi/supplementary_data/NON_OA/PMC1953900-PMC1957899/PMC1955304.zip"),
        None,
        None,
        None
      )
      val targetPath = handler.getLocalToRemoteTargetUpdatePath(origin)
      val result = targetPath(foundDoc)
      assert(result.get == "remote/ftp/a.site/a/path/to/aFile.txt")
    }

    "return a relative doclib path for local files with an HTTP remote origin" in {
      val origin: Origin = Origin(
        scheme = "http",
        hostname = None,
        uri = Some(Uri.parse("http://a.site/a/path/to/aFile.txt")),
        metadata = None,
        headers = None
      )
      val foundDoc = handler.FoundDoc(
        doc = createNewDoc("ingress/ebi/supplementary_data/NON_OA/PMC1953900-PMC1957899/PMC1955304.zip"),
        None,
        None,
        None
      )
      val targetPath = handler.getLocalToRemoteTargetUpdatePath(origin)
      val result = targetPath(foundDoc)
      assert(result.get == "remote/http/a.site/a/path/to/aFile.txt")
    }

    "return a relative doclib path for local files with an HTTPS remote origin" in {
      val origin: Origin = Origin(
        scheme = "https",
        hostname = None,
        uri = Some(Uri.parse("https://a.site/a/path/to/aFile.txt")),
        metadata = None,
        headers = None
      )
      val foundDoc = handler.FoundDoc(
        doc = createNewDoc("ingress/ebi/supplementary_data/NON_OA/PMC1953900-PMC1957899/PMC1955304.zip"),
        None,
        None,
        None
      )
      val targetPath = handler.getLocalToRemoteTargetUpdatePath(origin)
      val result = targetPath(foundDoc)
      assert(result.get == "remote/https/a.site/a/path/to/aFile.txt")
    }

    "a prefetch message can have multiple origins" in {
      val origins: List[Origin] = List(Origin(
        scheme = "mongodb",
        hostname = None,
        uri = Some(Uri.parse("remote/https/parent1")),
        metadata = Some(List(MetaString("_id", "1"))),
        headers = None
      ),
        Origin(
          scheme = "mongodb",
          hostname = None,
          uri = Some(Uri.parse("local/file/parent2")),
          metadata = Some(List(MetaString("_id", "2"))),
          headers = None
        ),
        Origin(
          scheme = "file",
          hostname = None,
          uri = Some(Uri.parse("local/file/parent3")),
          metadata = Some(List(MetaString("_id", "3"))),
          headers = None
        ),
        Origin(
          scheme = "http",
          hostname = Some("www.bbc.co.uk"),
          uri = Some(Uri.parse("http://www.bbc.co.uk/news")),
          headers = None,
          metadata = None
        )
      )

      assert(origins.count(origin => origin.scheme == "mongodb") == 2)
    }

    "A parent prefetch message which has derivative false should not be processed" in {
      val metadataMap: List[MetaString] = List(MetaString("doi", "10.1101/327015"))
      val prefetchMsg: PrefetchMsg = PrefetchMsg("/a/file/somewhere.pdf", None, Some(List("a-tag")), Some(metadataMap), Some(false))
      val result = Await.result(handler.processParent(createNewDoc("/a/file/somewhere.pdf"), prefetchMsg), 2 seconds)
      assert(result.isEmpty)
    }

    "A parent prefetch message with no derivative field should not be processed" in {
      val metadataMap: List[MetaString] = List(MetaString("doi", "10.1101/327015"))
      val prefetchMsg: PrefetchMsg = PrefetchMsg("/a/file/somewhere.pdf", None, Some(List("a-tag")), Some(metadataMap), None)
      val result = Await.result(handler.processParent(createNewDoc("/a/file/somewhere.pdf"), prefetchMsg), 2 seconds)
      assert(result.isEmpty)
    }

    "A prefetch message can have derivative type in the metadata" in {
      val metadataMap: List[MetaString] = List(MetaString("derivative.type", "unarchive"))
      val prefetchMsg: PrefetchMsg = PrefetchMsg("/a/file/somewhere.pdf", None, Some(List("a-tag")), Some(metadataMap), None)
      val derivMetadata = prefetchMsg.metadata.get.filter(p ⇒ p.getKey == "derivative.type")
      assert(derivMetadata.length == 1)
      assert(derivMetadata.head.getKey == "derivative.type")
      assert(derivMetadata.head.getValue == "unarchive")
    }
  }

  "An invalid URI should fail to convert" in {
    val path = "ingress/derivatives/derivatives/derivatives/remote/ftp/ftp.ebi.ac.uk/pub/databases/pmc/suppl/OA/PMC3621900-PMC3625899/unarchived_PMC3624804.zip/unarchived_supp_btt083_SPEDRE_SourceFiles.zip/unarchived_SPEDRE_SourceFiles.zip/Supplementary Source Files/Akt model/Noise=10%/GA_LM_Akt.cps"
    val result = handler.toUri(path)
    assert(result.raw == path)
    assert(result.uri.isEmpty)
  }

}