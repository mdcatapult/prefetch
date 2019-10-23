package io.mdcatapult.doclib.util

import java.io.{File, FileInputStream}
import java.security.{DigestInputStream, MessageDigest}
import java.math.BigInteger

import io.lemonlabs.uri.Uri
import better.files.{File ⇒ ScalaFile, _}

trait FileHash {
  /**
    * Generates and MD5 hash of the file contents
    * @param source Filepath to hash
    * @return
    */
  def md5(source: String): String = {
    val buffer = new Array[Byte](8192)
    val md5 = MessageDigest.getInstance("MD5")
    val dis = new DigestInputStream(new FileInputStream(new File(source)), md5)
    try {
      while (dis.read(buffer) != -1) {}
    } finally {
      dis.close()
    }
    md5.digest.map("%02x".format(_)).mkString
  }

  def md5HashString(s: String): String = {
    val md = MessageDigest.getInstance("MD5")
    val digest = md.digest(s.getBytes)
    val bigInt = new BigInteger(1,digest)
    val hashedString = bigInt.toString(16)
    hashedString
  }

  /**
   * Check the original file name against the new (possibly query hashed) one and if too long
   * then hash the orig name so we end up with name like 134636346.2463547536.pdf
   * @param origUri
   * @param fileName
   * @return
   */
  def hashOrOriginal(origUri: Uri, fileName: String): String = {
    fileName.length match {
      case length if length >= 256 ⇒ {
        val origFile = ScalaFile(origUri.path.toString())
        val origFileName = origFile.nameWithoutExtension
        val origExtension = origFile.extension.getOrElse("").replaceFirst(".", "")
        val queryHash = fileName.replace(origFileName, "").replace(origExtension, "").replace(".", "")
        s"${newFileName(origFileName, queryHash, origExtension)}"
      }
      case _ ⇒ fileName
    }
  }

  /**
   * Given a filename, hash of the url query params and a file extension it
   * returns new file name in form fileNameHash.queryHash.fileExtension
   * @param fileName
   * @param queryHash
   * @param fileExtension
   * @return
   */
  def newFileName(fileName: String, queryHash: String, fileExtension: String): String = {
    s"${md5HashString(fileName)}${
      queryHash match {
        case "" ⇒ ""
        case (value) ⇒ s".$value"
      }
    }${
      fileExtension match {
        case "" ⇒ ""
        case (value) ⇒ s".$value"
      }
    }"
  }
}
