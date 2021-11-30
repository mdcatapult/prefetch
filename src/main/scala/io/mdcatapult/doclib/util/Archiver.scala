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

/**
 * Sends messages to the archive queue, moves existing files to archive location.
 *
 * @param archiver
 * @param fileProcessor
 * @param executionContext
 */
class Archiver(archiver: Sendable[DoclibMsg], fileProcessor: FileProcessor)
(implicit executionContext: ExecutionContext) extends LazyLogging {

  // Alias for the mongo id as a string
  type DocumentID = String
  case class ArchiverError(documentID: DocumentID, exception: Throwable)

  type ArchiveResult = Either[ArchiverError, DocumentID]

  // Path is the new version of the file. archivedDocuments is the result of queuing messages move all the old files - not the actual move results
  case class ArchiveDocumentResult(path: Path, archivedDocuments: List[ArchiveResult])

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
   * @return ArchiveDocumentResult contains the path of the new doc and resutls of queuing messages to archive the old ones
   */
  def archiveDocument(foundDoc: FoundDoc, temp: String, archiveTarget: String, target: Option[String] = None): Future[Option[ArchiveDocumentResult]] = {
    val archiveSource = target.getOrElse(foundDoc.doc.source)
    logger.info(s"Archive ${foundDoc.archiveable.map(d => d._id).mkString(",")} source=$archiveSource target=$archiveTarget")
    (for {
      _ <- OptionT.fromOption[Future](fileProcessor.copyFile(archiveSource, archiveTarget))
      archivedDocs = sendDocumentsToArchiver(foundDoc.archiveable)
      movedFilePath <- OptionT.fromOption(fileProcessor.moveFile(temp, archiveSource))
    } yield ArchiveDocumentResult(movedFilePath, archivedDocs)).value
  }

  /**
   * Sends documents to archiver for processing.
   * Returns a list of docs that got sent successfully and others that failed
   *
   * @param docs
   * @return List[ArchiveResult] Result of queuing the messages to move existing versions of a doc
   */
  private def sendDocumentsToArchiver(docs: List[DoclibDoc]): List[ArchiveResult] = {
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
   * @return ArchiveResult Which is really Either[ArchiverError, String] where it can contain a Left[String, Throwable]
   *         ie id of the doc and an exception if the message queue failed or Right[String] ie the id of the doc it is succeeded
   */
  private def sendMessage(id: String): ArchiveResult = {
    Try (archiver.send(DoclibMsg(id))) match {
      case Success(_) =>
        logger.info(s"Sent documents to archiver: ${id}")
        Right(id)
      case Failure(e) =>
        logger.error("failed to send doc to archive", e)
        Left(ArchiverError(id, e))
    }
  }

}
