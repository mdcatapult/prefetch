package io.mdcatapult.doclib.remote.adapters

import akka.stream.Materializer
import com.typesafe.config.Config
import io.lemonlabs.uri._
import io.mdcatapult.doclib.models.Origin
import io.mdcatapult.doclib.remote.DownloadResult
import io.mdcatapult.doclib.util.HashUtils.md5
import org.apache.tika.mime.MimeTypesFactory

object Adapter {

  private val mimeTypes = MimeTypesFactory.create(getClass.getResource("/org/apache/tika/mime/tika-mimetypes.xml"))

  def contentTypeExtension(contentType: String): String =
    mimeTypes.forName(contentType).getExtension
}

trait Adapter {

  def unapply(origin: Origin)(implicit config: Config, m: Materializer): Option[DownloadResult]
  def download(origin: Origin)(implicit config: Config, m: Materializer): Option[DownloadResult]

  /**
    * generate path on filesystem from uri
    *
    * generates a url while attempting to maintain file extensions and
    * generation of index filename for uris ending in slash.
    * If uri includes query string it will generate an MD5 hash in the filename
    *
    * @param origin io.mdcatapult.doclib.models.Origin
    * @param root root of path to generate
    * @param fileName optional filename which, if defined, will replace the last part of the uri
    * @return
    */
  def generateFilePath(origin: Origin, root: Option[String] = None, fileName: Option[String], contentType: Option[String]): String = {
    val targetDir = root.getOrElse("").replaceAll("/+$", "")
    val uri = origin.uri.get
    val query = uri.toUrl.query

    val queryHash =
      if (query.nonEmpty)
        "." + md5(s"?$query")
      else
        ""

    def getDisposition: (Boolean, String) = {
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

    def insertQueryHash(pathEnd: String): String = {
      val hasExtension = """(.*)\.(.*)$""".r
      val headerExt = contentType.map(Adapter.contentTypeExtension).getOrElse(".html")

      pathEnd match {
        case "" => s"index$queryHash$headerExt"
        case hasExtension(p, ext) => s"$p$queryHash.$ext"
        case p => s"$p$queryHash$headerExt"
      }
    }

    def generateBasename(path: Path): String = {
      val allParts = path.parts ++ fileName.toVector
      var lastPathPart = insertQueryHash(allParts.last)
      val (hasDisposition, disposition) = getDisposition
      if (hasDisposition && fileName.isEmpty) lastPathPart = disposition
      "/" + (allParts.init.filter(_.nonEmpty) ++ Vector(lastPathPart)).mkString("/")
    }

    s"$targetDir/${
      uri.schemeOption match {
        case Some(scheme) => s"$scheme/"
        case None => ""
      }
    }${
      uri.toUrl.hostOption.getOrElse("")
    }${
      uri.path match {
        case EmptyPath => s"${generateBasename(Path.parse("/index.html"))}"
        case path: RootlessPath => s"${generateBasename(path)}"
        case path: AbsolutePath => generateBasename(path)
        case _ => ""
      }
    }"
  }
}
