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

class Archiver(archiver: Sendable[DoclibMsg], fileProcessor: FileProcessor)
              (implicit executionContext: ExecutionContext) extends LazyLogging {

  /**
   * updates a physical file
   *  - copies existing file to archive location
   *  - adds document to archive collection
   *  - moves new file to target/document-source location
   *
   * @param foundDoc      FoundDoc
   * @param temp          path that the new file is located at
   * @param archiveTarget the path that the file needs to be copied to
   * @param target        an optional path to set the new source to if not using the source from the document
   * @return path of the target/document-source location
   */
  def archiveDocument(foundDoc: FoundDoc, temp: String, archiveTarget: String, target: Option[String] = None): Future[Option[(Path, List[Either[String, String]])]] = {
    val archiveSource = target.getOrElse(foundDoc.doc.source)
    logger.info(s"Archive ${foundDoc.archiveable.map(d => d._id).mkString(",")} source=$archiveSource target=$archiveTarget")
    (for {
      _ <- OptionT.fromOption[Future](fileProcessor.copyFile(archiveSource, archiveTarget))
      archivedDocs = sendDocumentsToArchiver(foundDoc.archiveable)
      movedFilePath <- OptionT.fromOption(fileProcessor.moveFile(temp, archiveSource))
    } yield (movedFilePath, archivedDocs)).value
  }

  /**
   * Sends documents to archiver for processing.
   * Returns a list of docs that got sent successfully and others that failed
   * 
   * @param docs
   * @return
   */
  private def sendDocumentsToArchiver(docs: List[DoclibDoc]): List[Either[String, String]] = {
    val messageSuccess = {
      for {
        doc <- docs
        id = doc._id.toHexString
        successOrFail = sendMessage(id)
      } yield successOrFail
    }
    messageSuccess
  }

  /**
   * Send a single message to the archiver with the id of
   * the document to be archived. Return an Either with the id in a Left or Right
   * depending on whether success(R) or fail(L)
   * @param id
   * @return Either[String, String]
   */
  private def sendMessage(id: String): Either[String, String] = {
    Try (archiver.send(DoclibMsg(id))) match {
      case Success(_) =>
        logger.info(s"Sent documents to archiver: ${id}")
        Right(id)
      case Failure(e) =>
        logger.error("failed to send doc to archive", e)
        Left(id)
    }
  }

}
