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

import java.io.File
import java.nio.file.Paths
import org.apache.pekko.stream.Materializer
import com.typesafe.config.Config
import io.lemonlabs.uri._
import io.mdcatapult.doclib.models.Origin
import io.mdcatapult.doclib.remote.DownloadResult
import io.mdcatapult.util.hash.Md5.md5
import org.apache.tika.mime.MimeTypesFactory

import scala.concurrent.Future

object Adapter {

  private val mimeTypes = MimeTypesFactory.create(getClass.getResource("/org/apache/tika/mime/tika-mimetypes.xml"))

  val _protocols = List()

  def contentTypeExtension(contentType: String): String =
    mimeTypes.forName(contentType).getExtension

  def protocols:List[String] = _protocols
}

trait Adapter {

  def download(origin: Origin)(implicit config: Config, m: Materializer): Future[Option[DownloadResult]]

  /**
    * Generates a file path based on the file origin using the scheme, host, path, location header and content disposition header.
    * content-disposition overrides the Location header.
    *
    * @param origin io.mdcatapult.doclib.models.Origin
    * @param root root of path to generate
    * @param fileName optional filename which, if defined, will replace the last part of the uri
    * @return
    */
  def generateFilePath(origin: Origin, root: Option[String] = None, fileName: Option[String], contentType: Option[String]): String = {
    val targetDir = root.getOrElse("").replaceAll("/+$", "")
    val uri = origin.uri.get
    val (hasDisposition, _) = getDisposition(origin)

    s"$targetDir${File.separator}${
      uri.schemeOption match {
        case Some(scheme) => s"$scheme"
        case None => ""
      }
    }${
      if (hasDisposition) {
        s"${File.separator}${uri.toUrl.hostOption.getOrElse("")}"
      } else {
        getLocation(origin) match {
          case (true, locationPath) => {
            val parsedLocation = Uri.parse(locationPath.getOrElse(List("")).head)
            parsedLocation match {
              case path: RelativeUrl => {
                if (path.toString() == "" || path.toString() == "/") {
                  s"${File.separator}${uri.toUrl.hostOption.getOrElse("")}"
                } else {
                  s"${File.separator}${uri.toUrl.hostOption.getOrElse("")}${File.separator}${path.toString().stripPrefix("/")}"
                }
              }
              case _: AbsoluteUrl => s"${File.separator}${parsedLocation.toUrl.hostOption.getOrElse("")}"
              case _ => ""
            }
          }
          case (false, _) => s"${File.separator}${uri.toUrl.hostOption.getOrElse("")}"
        }
      }
    }${
      uri.path match {
        case EmptyPath => s"${generateBasename(Path.parse("/index.html"), origin, fileName, contentType)}"
        case path: RootlessPath => s"${generateBasename(path, origin, fileName, contentType)}"
        case path: AbsolutePath => generateBasename(path, origin, fileName, contentType)
        case _ => ""
      }
    }"
  }

  /**
   * Returns the doclib filepath without the scheme, host, or prefix.
   *
   * @param path io.lemonlabs.uri.Path
   * @param origin io.mdcatapult.doclib.models.Origin
   * @param fileName optional filename which, if defined, will replace the last part of the uri
   * @param contentType optional filename override
   * @return
   */
  def generateBasename(path: Path, origin: Origin, fileName: Option[String], contentType: Option[String]): String = {
    val allParts = path.parts ++ fileName.toVector
    val (hasDisposition, disposition) = getDisposition(origin)
    val lastPathPart = if (hasDisposition && (allParts.last.equals("") || allParts.last.equals("index.html"))) {
      disposition
    } else if (hasDisposition && fileName.isEmpty) {
      Vector(allParts.last.stripSuffix(File.separator), disposition).mkString({File.separator})
    } else {
      insertQueryHash(allParts.last, origin.uri.get.toUrl.query, contentType)
    }
    Paths.get(File.separator, allParts.init.filter(_.nonEmpty).mkString(File.separator), lastPathPart).toString
  }

  /**
   * Given the final part of a path, a query string, and the content type, returns a filename with an extension and an
   * md5 hash of the query string.
   *
   * @param pathEnd String
   * @param query io.lemonlabs.uri.QueryString
   * @param contentType optional filename override
   * @return
   */
  def insertQueryHash(pathEnd: String, query: QueryString, contentType: Option[String]): String = {
    val queryHash =
      if (query.nonEmpty)
        "." + md5(s"?$query")
      else
        ""

    val hasExtension = """(.*)\.(.*)$""".r
    val headerExt = contentType.map(Adapter.contentTypeExtension).getOrElse(".html")

    pathEnd match {
      case "" => s"index$queryHash$headerExt"
      case hasExtension(p, ext) => s"$p$queryHash.$ext"
      case p => s"$p$queryHash$headerExt"
    }
  }

  /**
   * Extracts content-disposition information from the origin and sanitizes the filename if present.
   *
   * @param origin io.mdcatapult.doclib.models.Origin file origin
   * @return
   */
  def getDisposition(origin: Origin): (Boolean, String) = {
    val hasFilename = """(filename|FILENAME)[^;=\n]*=((['"]).*?\3|[^;\n]*)""".r
    val dispositionHeaderValue = {
      origin.headers.getOrElse(Map()).get("Content-Disposition") match {
        case Some(x) => x
        case _ =>
          origin.headers.getOrElse(Map()).get("content-disposition") match {
            case Some(x) => x
            case _ =>
              return (false, "")
          }
      }
    }

    val f = (for {
      v <- dispositionHeaderValue
      regexMatch <- hasFilename.findAllMatchIn(v)
      file = s"${regexMatch.group(2)}"
    } yield file).headOption

    f match {
      case Some(x) =>
        val file = x.stripPrefix("UTF-8''")
          .stripPrefix("utf-8''")
          .stripPrefix("\"")
          .stripSuffix("\"")
          .replace(" ", "-")
        (true, file)
      case _ => (false, "")
    }
  }

  /**
   * Return the "Location" header from the http response
   * @param origin
   * @return
   */
  def getLocation(origin: Origin): (Boolean, Option[Seq[String]]) = {
    origin.headers.getOrElse(Map()).get("Location") match {
      case Some(x) => (true, Some(x))
      case _ => (false, None)
    }
  }
}
