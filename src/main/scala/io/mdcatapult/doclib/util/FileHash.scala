package io.mdcatapult.doclib.util

import better.files.{File => ScalaFile}
import io.lemonlabs.uri.Uri
import io.mdcatapult.util.hash.Md5.md5

object FileHash {

  /**
   * Check the original file name against the new (possibly query hashed) one and if too long
   * then hash the orig name so we end up with name like 134636346.2463547536.pdf
   * @return name of file that is no longer than 256 characters
   */
  def hashOrOriginal(origUri: Uri, fileName: String): String = {
    fileName.length match {
      case length if length >= 256 =>
        val origFile = ScalaFile(origUri.path.toString())
        val origFileName = origFile.nameWithoutExtension
        val origExtension = origFile.extension.getOrElse("").replaceFirst(".", "")
        val queryHash = fileName.replace(origFileName, "").replace(origExtension, "").replace(".", "")
        s"${newFileName(origFileName, queryHash, origExtension)}"
      case _ => fileName
    }
  }

  /**
   * Given a filename, hash of the url query params and a file extension it
   * returns new file name in form fileNameHash.queryHash.fileExtension
   *
   * @return
   */
  private def newFileName(fileName: String, queryHash: String, fileExtension: String): String =
    s"${md5(fileName)}${
      queryHash match {
        case "" => ""
        case value => s".$value"
      }
    }${
      fileExtension match {
        case "" => ""
        case value => s".$value"
      }
    }"
}
