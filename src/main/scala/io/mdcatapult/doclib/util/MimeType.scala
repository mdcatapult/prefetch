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
