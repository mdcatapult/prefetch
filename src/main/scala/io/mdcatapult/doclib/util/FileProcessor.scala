package io.mdcatapult.doclib.util

import io.mdcatapult.doclib.util.Metrics.fileOperationLatency

import java.io.{File, FileNotFoundException}
import java.nio.file.{Files, Path, Paths, StandardCopyOption}
import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

class FileProcessor(doclibRoot: String) {

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

  def moveFile(sourcePath: String, target: String): Option[Path] = {
    val source = Paths.get(s"$doclibRoot$sourcePath").toAbsolutePath.toFile
    val destination = Paths.get(s"$doclibRoot$target").toAbsolutePath.toFile

    val result = Try({
      if (source == destination) {
        destination.toPath
      } else {
        destination.getParentFile.mkdirs
        val latency = fileOperationLatency.labels("move").startTimer()
        val path = Files.move(source.toPath, destination.toPath, StandardCopyOption.REPLACE_EXISTING)
        latency.observeDuration()
        path
      }
    })
      result match {
      case Success(_) => Some(destination.toPath)
      case Failure(err) => throw err
    }
  }


  /**
   * Copies a file to a new location
   *
   * @param source source path
   * @param target target path
   * @return
   */
  def copyFile(sourcePath: String, targetPath: String): Option[Path] = {

    val source = Paths.get(s"$doclibRoot$sourcePath").toAbsolutePath.toFile
    val target = Paths.get(s"$doclibRoot$targetPath").toAbsolutePath.toFile
    target.getParentFile.mkdirs
    val latency = fileOperationLatency.labels("copy").startTimer()

    val path =
      Try({
        Files.copy(source.toPath, target.toPath, StandardCopyOption.REPLACE_EXISTING)
        latency.observeDuration()
      })

    path match {
      case Success(_) => Some(Paths.get(targetPath))
      case Failure(_: FileNotFoundException) => None
      case Failure(err) => throw err
    }
  }

  /**
   * Silently remove file and empty parent dirs
   *
   * @param source String
   */
  def removeFile(source: String): Unit = {
    val file = Paths.get(s"$doclibRoot$source").toAbsolutePath.toFile
    val latency = fileOperationLatency.labels("remove").startTimer()
    remove(Option(file))
    latency.observeDuration()

    @tailrec
    def remove(o: Option[File]): Unit = {
      o match {
        case Some(f) if f.isFile || Option(f.listFiles()).exists(_.isEmpty) =>
          file.delete()
          remove(Option(f.getParentFile))
        case _ => ()
      }
    }
  }
}
