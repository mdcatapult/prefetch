package io.mdcatapult.doclib.util

import better.files.Dsl.pwd
import org.scalatest.flatspec.AnyFlatSpec

import java.io.File
import java.nio.file.{Files, NoSuchFileException, Paths}

class FileProcessorSpec extends AnyFlatSpec {

  val doclibRoot = "doclib"
  val fileProcessor = new FileProcessor(doclibRoot)
  val path = "/a/path/to/a/file.txt"

  private def getSourceAndTarget(): (String, String) = {
    Files.createDirectories(Paths.get(s"$pwd/$doclibRoot/tmp"))
    val targetfileName = File.createTempFile("target", ".txt", new File(s"$pwd/$doclibRoot/tmp")).getName
    val targetPath = s"/tmp/$targetfileName"

    val sourcefileName = File.createTempFile("source", ".txt", new File(s"$pwd/$doclibRoot/tmp")).getName
    val sourcePath = s"/tmp/$sourcefileName"
    (sourcePath, targetPath)
  }

  "Moving a non existent file" should "throw an exception" in {
    assertThrows[NoSuchFileException] {
      fileProcessor.moveFile("/a/file/that/does/no/exist.txt", "./aFile.txt")
    }
  }

  "Moving a file with the same source and target" should "return the original file path" in {
    val src = File.createTempFile("source", ".txt")
    val actualPath = fileProcessor.moveFile(src.getPath, src.getPath)
    assert(actualPath.get.toString == s"$pwd/$doclibRoot${src.getPath}")
  }

  "Moving a file from source to target" should "return the new file path" in {
    val (sourcePath, targetPath) = getSourceAndTarget()
    val actualPath = fileProcessor.moveFile(sourcePath, targetPath)
    assert(actualPath.get.toString == s"$pwd/$doclibRoot${targetPath}")
  }

  "Copying a file from source to target" should "copy the new file" in {
    val (sourcePath, targetPath) = getSourceAndTarget()
    val filePath = fileProcessor.copyFile(sourcePath, targetPath)
    assert(filePath.get.toString == targetPath)
  }

  "Copying a non existent file" should "throw an exception" in {
    assertThrows[NoSuchFileException] {
      fileProcessor.copyFile("/a/file/that/does/no/exist.txt", "./aFile.txt")
    }
  }

  "Removing a file from source" should "remove the new file" in {
    val source = File.createTempFile("source", ".txt", new File(s"$pwd/$doclibRoot/tmp"))
    val sourceFileName = source.getName
    fileProcessor.removeFile(s"/tmp/$sourceFileName")

    assert(!Files.exists(source.toPath))
  }

  "Removing a file that doesn't exist" should "not remove the new file" in {
    val source = File.createTempFile("source", ".txt", new File(s"$pwd/$doclibRoot/tmp"))
    source.deleteOnExit()
    val sourceFileName = source.getName
    fileProcessor.removeFile(s"/tmp/$sourceFileName")
    assert(!Files.exists(source.toPath))
  }
}
