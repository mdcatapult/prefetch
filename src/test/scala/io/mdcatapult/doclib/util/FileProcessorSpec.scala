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

import org.scalatest.flatspec.AnyFlatSpec

import java.io.File
import java.nio.file.{Files, NoSuchFileException}

class FileProcessorSpec extends AnyFlatSpec {

  val fileProcessor = new FileProcessor("")
  val path = "/a/path/to/a/file.txt"

  "Moving a non existent file" should "throw an exception" in {
    assertThrows[NoSuchFileException] {
      fileProcessor.moveFile("/a/file/that/does/no/exist.txt", "./aFile.txt")
    }
  }

  "Moving a file with the same source and target" should "return the original file path" in {
    val actualPath = fileProcessor.moveFile(new File(path).getPath, new File(path).getPath)
    assert(actualPath.get == new File(path).toPath)
  }

  "Moving a file from source to target" should "return the new file path" in {
    val target = "tmp/target.txt"
    val src = File.createTempFile("source", ".txt")
    val actualPath = fileProcessor.moveFile(src.getPath, target)
    assert(actualPath.get.toString == target)
  }

  "Copying a file from source to target" should "copy the new file" in {
    val target = "tmp/target.txt"
    val src = File.createTempFile("source", ".txt")
    val filePath = fileProcessor.copyFile(src.getPath, target)
    assert(filePath.get.toString == target)
  }

  "Copying a non existent file" should "throw an exception" in {
    assertThrows[NoSuchFileException] {
      fileProcessor.copyFile("/a/file/that/does/no/exist.txt", "./aFile.txt")
    }
  }

  "Removing a file from source" should "remove the new file" in {
    val src = File.createTempFile("source", ".txt")
    fileProcessor.removeFile(src.getPath)

    assert(!Files.exists(src.toPath))
  }

  "Removing a file that doesn't exist" should "not remove the new file" in {
    val src = File.createTempFile("source", ".txt")
    src.deleteOnExit()
    fileProcessor.removeFile(src.getPath)
    assert(!Files.exists(src.toPath))
  }
}
