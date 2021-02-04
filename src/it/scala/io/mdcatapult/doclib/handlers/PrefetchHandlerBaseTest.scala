package io.mdcatapult.doclib.handlers

import better.files.Dsl.pwd
import com.typesafe.config.{Config, ConfigFactory}
import io.mdcatapult.doclib.codec.MongoCodecs
import io.mdcatapult.doclib.messages.{DoclibMsg, PrefetchMsg}
import io.mdcatapult.doclib.models.{DoclibDoc, ParentChildMapping}
import io.mdcatapult.klein.mongo.Mongo
import io.mdcatapult.klein.queue.Sendable
import io.mdcatapult.util.concurrency.SemaphoreLimitedExecution
import io.mdcatapult.util.path.DirectoryDeleter.deleteDirectories
import org.bson.codecs.configuration.CodecRegistry
import org.mongodb.scala.MongoCollection
import org.scalamock.scalatest.MockFactory
import org.scalatest.BeforeAndAfterAll

import scala.concurrent.ExecutionContext.Implicits.global

trait PrefetchHandlerBaseTest extends MockFactory with BeforeAndAfterAll {

  implicit val config: Config = ConfigFactory.parseString(
    s"""
       |doclib {
       |  root: "$pwd/test/prefetch-test"
       |  remote {
       |    target-dir: "remote"
       |    temp-dir: "remote-ingress"
       |  }
       |  local {
       |    target-dir: "local"
       |    temp-dir: "ingress"
       |  }
       |  archive {
       |    target-dir: "archive"
       |  }
       |  derivative {
       |    target-dir: "derivatives"
       |  }
       |}
       |mongo {
       |  doclib-database: "prefetch-test"
       |  documents-collection: "documents"
       |  derivatives-collection : "derivatives"
       |}
    """.stripMargin).withFallback(ConfigFactory.load())

  /** Initialise Mongo * */
  implicit val codecs: CodecRegistry = MongoCodecs.get
  val mongo: Mongo = new Mongo()

  implicit val collection: MongoCollection[DoclibDoc] = mongo.getCollection(config.getString("mongo.doclib-database"), config.getString("mongo.documents-collection"))
  implicit val derivativesCollection: MongoCollection[ParentChildMapping] = mongo.getCollection(config.getString("mongo.doclib-database"), config.getString("mongo.derivatives-collection"))

  implicit val upstream: Sendable[PrefetchMsg] = stub[Sendable[PrefetchMsg]]
  val downstream: Sendable[DoclibMsg] = stub[Sendable[DoclibMsg]]
  val archiver: Sendable[DoclibMsg] = stub[Sendable[DoclibMsg]]

  val readLimiter: SemaphoreLimitedExecution = SemaphoreLimitedExecution.create(config.getInt("mongo.read-limit"))
  val writeLimiter: SemaphoreLimitedExecution = SemaphoreLimitedExecution.create(config.getInt("mongo.write-limit"))

  override def afterAll(): Unit = {
    //    Await.result(collection.drop().toFutureOption(), 5.seconds)
    //    Await.result(derivativesCollection.drop().toFutureOption(), 5.seconds)
    // These may or may not exist but are all removed anyway
    deleteDirectories(List(pwd / "test" / "prefetch-test"))
  }

}