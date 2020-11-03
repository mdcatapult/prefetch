package io.mdcatapult.doclib.util

import java.io.{File, FileInputStream}

import org.apache.tika.Tika
import org.apache.tika.io.TikaInputStream
import org.apache.tika.metadata.{Metadata, TikaMetadataKeys}

object MimeType {

  private val tika: Tika = new Tika()

  def getMimetype(filePath: String): String = {
    val metadata = new Metadata()
    val actualFile = new File(filePath)
    metadata.set(TikaMetadataKeys.RESOURCE_NAME_KEY, actualFile.getName)
    tika.getDetector.detect(
      TikaInputStream.get(new FileInputStream(actualFile.getAbsolutePath)),
      metadata
    ).toString
  }

}
