package io.mdcatapult.doclib.util

import cats.data.OptionT
import cats.implicits.catsStdInstancesForFuture
import com.typesafe.scalalogging.LazyLogging
import io.mdcatapult.doclib.handlers.FoundDoc
import io.mdcatapult.doclib.messages.DoclibMsg
import io.mdcatapult.doclib.models.DoclibDoc
import io.mdcatapult.doclib.util.Metrics.fileOperationLatency
import io.mdcatapult.klein.queue.Sendable

import java.io.{File, FileNotFoundException}
import java.nio.file.{Files, Path, Paths, StandardCopyOption}
import scala.annotation.tailrec
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

class FileProcessor(doclibRoot: String, archiver: Sendable[DoclibMsg])(implicit executionContext: ExecutionContext) extends LazyLogging {

  def process(newHash: String,
              oldHash: String,
              tempPath: String,
              targetPath: String,
              isDerivative: Boolean,
              inRightLocation: Boolean
             ): Option[Path] = {
    if (newHash != oldHash && isDerivative || !inRightLocation) {
      // File already exists at target location but is not the same file.
      // Overwrite it and continue because we don't archive derivatives.
      moveFile(tempPath, targetPath)
    } else { // not a new file or a file that requires updating so we will just cleanup the temp file
      removeFile(tempPath)
      None
    }
  }

  /**
   * moves a file on the file system from its source path to an new root location maintaining the path and prefixing the filename
   *
   * @param source current relative source path
   * @param target relative target path to move file to
   * @return
   */
  def moveFile(source: String, target: String): Option[Path] = moveFile(
    Paths.get(s"$doclibRoot$source").toAbsolutePath.toFile,
    Paths.get(s"$doclibRoot$target").toAbsolutePath.toFile
  ) match {
    case Success(_) => Some(Paths.get(target))
    case Failure(err) => throw err
  }

  /**
   * moves a file on the file system from its source path to an new root location maintaining the path and prefixing the filename
   *
   * @param source current absolute source path
   * @param target absolute target path to move file to
   * @return
   */

  def moveFile(source: File, target: File): Try[Path] = {
    Try({
      if (source == target) {
        target.toPath
      } else {
        target.getParentFile.mkdirs
        val latency = fileOperationLatency.labels("move").startTimer()
        val path = Files.move(source.toPath, target.toPath, StandardCopyOption.REPLACE_EXISTING)
        latency.observeDuration()
        path
      }
    })
  }

  /**
   * Copies a file to a new location
   *
   * @param source source path
   * @param target target path
   * @return
   */
  def copyFile(source: String, target: String): Option[Path] = copyFile(
    Paths.get(s"$doclibRoot$source").toAbsolutePath.toFile,
    Paths.get(s"$doclibRoot$target").toAbsolutePath.toFile
  ) match {
    case Success(_) => Some(Paths.get(target))
    case Failure(_: FileNotFoundException) => None
    case Failure(err) => throw err
  }

  /**
   * Copies a file to a new location
   *
   * @param source source path
   * @param target target path
   * @return
   */
  def copyFile(source: File, target: File): Try[Path] =
    Try({
      target.getParentFile.mkdirs
      val latency = fileOperationLatency.labels("copy").startTimer()
      val path = Files.copy(source.toPath, target.toPath, StandardCopyOption.REPLACE_EXISTING)
      latency.observeDuration()
      path
    })

  /**
   * Silently remove file and empty parent dirs
   *
   * @param source String
   */
  def removeFile(source: String): Unit = {
    val file = Paths.get(s"$doclibRoot$source").toAbsolutePath.toFile
    val latency = fileOperationLatency.labels("remove").startTimer()
    removeFile(file)
    latency.observeDuration()
  }

  /**
   * Silently remove file and empty parent dirs
   *
   * @param file File
   */
  def removeFile(file: File): Unit = {

    @tailrec
    def remove(o: Option[File]): Unit = {
      o match {
        case Some(f) if f.isFile || Option(f.listFiles()).exists(_.isEmpty) =>
          file.delete()
          remove(Option(f.getParentFile))
        case _ => ()
      }
    }
    remove(Option(file))
  }

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
  def updateFile(foundDoc: FoundDoc, temp: String, archive: String, target: Option[String] = None): Future[Option[Path]] = {
    val targetSource = target.getOrElse(foundDoc.doc.source)
    archiveDocument(foundDoc, targetSource, archive).map(_ => moveFile(temp, targetSource))
  }

  /**
   *
   * @param foundDoc      document to be archived
   * @param archiveSource string file to copy
   * @param archiveTarget string location to archive the document to
   * @return
   */
  def archiveDocument(foundDoc: FoundDoc, archiveSource: String, archiveTarget: String): Future[Option[Path]] = {
    logger.info(s"Archive ${foundDoc.archiveable.map(d => d._id).mkString(",")} source=$archiveSource target=$archiveTarget")
    (for {
      archivePath: Path <- OptionT.fromOption[Future](copyFile(archiveSource, archiveTarget))
      _ <- OptionT.liftF(sendDocumentsToArchiver(foundDoc.archiveable))
    } yield archivePath).value
  }

  /**
   * sends documents to archiver for processing.
   *
   * @todo send errors to queue without killing the rest of the process
   */
  def sendDocumentsToArchiver(docs: List[DoclibDoc]): Future[Unit] = {
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
