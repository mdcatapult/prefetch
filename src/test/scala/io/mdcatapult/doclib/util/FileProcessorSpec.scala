package io.mdcatapult.doclib.util

import org.scalatest.TryValues.convertTryToSuccessOrFailure
import org.scalatest.flatspec.AnyFlatSpec

import java.io.File

class FileProcessorSpec extends AnyFlatSpec {

  val fileProcessor = new FileProcessor("foobar")
  val path = "/a/path/to/a/file.txt"

  "Moving a non existent file" should "throw an exception" in {
    assertThrows[Exception] {
      fileProcessor.moveFile("/a/file/that/does/no/exist.txt", "./aFile.txt")
    }
  }

  "Moving a file with the same source and target" should "return the original file path" in {
    val actualPath = fileProcessor.moveFile(new File(path), new File(path))
    assert(actualPath.success.value == new File(path).toPath)
  }

//  "Moving a file from source to target" should "return the new file path" in {
//    val (source, target) = ("/source.txt", "/target.txt")
//    val src = new File(source)
//    src.createNewFile()
//    val actualPath = fileProcessor.moveFile(src.getPath, target)
//    assert(actualPath.toString == target)
//  }
}
