package io.mdcatapult.doclib.util

import cats.data.OptionT
import cats.implicits.catsStdInstancesForFuture
import com.typesafe.scalalogging.LazyLogging
import io.mdcatapult.doclib.handlers.FoundDoc
import io.mdcatapult.doclib.messages.DoclibMsg
import io.mdcatapult.doclib.models.DoclibDoc
import io.mdcatapult.klein.queue.Sendable

import java.nio.file.Path
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

class Archiver(archiver: Sendable[DoclibMsg], fileProcessor: FileProcessor)(implicit executionContext: ExecutionContext) extends LazyLogging {

  /**
   * updates a physical file
   *  - copies existing file to archive location
   *  - adds document to archive collection
   *  - moves new file to target/document-source location
   * @param foundDoc FoundDoc
   * @param temp path that the new file is located at
   * @param archive the path that the file needs to be copied to
   * @param target an optional path to set the new source to if not using the source from the document
   * @return path of the target/document-source location
   */
  def archiveDocument(foundDoc: FoundDoc, temp: String, archiveTarget: String, target: Option[String] = None): Future[Option[Path]] = {
    val archiveSource = target.getOrElse(foundDoc.doc.source)
    logger.info(s"Archive ${foundDoc.archiveable.map(d => d._id).mkString(",")} source=$archiveSource target=$archiveTarget")
    (for {
      archivePath: Path <- OptionT.fromOption[Future](fileProcessor.copyFile(archiveSource, archiveTarget))
      _ <- OptionT.liftF(sendDocumentsToArchiver(foundDoc.archiveable))
    } yield archivePath).value.map(_ => fileProcessor.moveFile(temp, archiveSource))
  }

  /**
   * sends documents to archiver for processing.
   *
   * @todo send errors to queue without killing the rest of the process
   */
  private def sendDocumentsToArchiver(docs: List[DoclibDoc]): Future[Unit] = {
    val messages =
      for {
        doc <- docs
        id = doc._id.toHexString
      } yield DoclibMsg(id)

    Try(messages.foreach(msg => archiver.send(msg))) match {
      case Success(_) =>
        logger.info(s"Sent documents to archiver: ${messages.map(d => d.id).mkString(",")}")
        Future.successful(())
      case Failure(e) =>
        logger.error("failed to send doc to archive", e)
        Future.successful(())
    }
  }

}
