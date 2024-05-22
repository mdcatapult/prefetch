/*
 * Copyright 2024 Medicines Discovery Catapult
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
  def moveFile(source: String, target: String): Option[Path] = {
    val sourceFile = Paths.get(s"$doclibRoot$source").toAbsolutePath.toFile
    val targetFile = Paths.get(s"$doclibRoot$target").toAbsolutePath.toFile

    Try({
      if (sourceFile == targetFile) {
        targetFile.toPath
      } else {

        targetFile.getParentFile.mkdirs
        val latency = fileOperationLatency.labels("move").startTimer()
        val path = Files.move(sourceFile.toPath, targetFile.toPath, StandardCopyOption.REPLACE_EXISTING)
        latency.observeDuration()
        path
      }
    }) match {
      case Success(_) => Some(Paths.get(target))
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
    latency.observeDuration()
  }
}
