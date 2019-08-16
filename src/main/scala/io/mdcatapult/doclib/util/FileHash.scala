package io.mdcatapult.doclib.util

import java.io.{File, FileInputStream}
import java.security.{DigestInputStream, MessageDigest}

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
}
