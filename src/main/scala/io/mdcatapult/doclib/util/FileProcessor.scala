package io.mdcatapult.doclib.util

import io.mdcatapult.doclib.util.Metrics.fileOperationLatency

import java.io.{File, FileNotFoundException}
import java.nio.file.{Files, Path, Paths, StandardCopyOption}
import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

class FileProcessor(doclibRoot: String) {

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
}
