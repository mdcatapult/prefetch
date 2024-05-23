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

package io.mdcatapult.doclib.remote.adapters

import org.apache.pekko.http.javadsl.model.HttpHeader

/** Extracting information from Http headers. */
object Headers {

  /** Extract the defined filename of the underlying document.  This uses Content-Disposition if available,
    * preferring filename* over filename as per
    * [[https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Content-Disposition Content-Disposition specification]]
    *
    * @param headers sequence of headers to examine.  If Content-Disposition appears twice then the first is taken
    * @return option with the file name in or None if filename is not defined
    */
  def filename(headers: Seq[HttpHeader]): Option[String] =
    headers.find(_.lowercaseName() == "content-disposition").flatMap { h =>

      val assignments: Array[(String, String)] =
        for {
          assignment <- h.value().split(";") if assignment.contains("=")
          nameValues = assignment.split("=", 2).map {_.trim}
        } yield nameValues(0) -> nameValues(1)

      val m = assignments.toMap

      m.get("filename*").orElse(m.get("filename")).map {unquote}
    }

  /** Extract the content type of the underlying document.  This uses Content-Type if available.
    *
    * @param headers sequence of headers to examine.  If Content-Type appears twice then the first is taken
    * @return option with the content type in or None if Content-Type is not defined
    */
  def contentType(headers: Seq[HttpHeader]): Option[String] =
    headers.find(_.lowercaseName() == "content-type").map(_.value())

  private def unquote(s: String): String =
    s.replaceAll("^\"|\"$", "")
}
