package io.mdcatapult.doclib.remote.adapters

import java.security.MessageDigest

import com.typesafe.config.Config
import io.lemonlabs.uri._
import io.mdcatapult.doclib.remote.DownloadResult
import org.apache.tika.mime.MimeTypesFactory

object Adapter {

  private val mimeTypes = MimeTypesFactory.create(getClass.getResource("/org/apache/tika/mime/tika-mimetypes.xml"))

  def contentTypeExtension(contentType: String): String =
    mimeTypes.forName(contentType).getExtension
}

trait Adapter {

  def unapply(uri: Uri)(implicit config: Config): Option[DownloadResult]
  def download(uri: Uri)(implicit config: Config): Option[DownloadResult]

  /**
    * generate path on filesystem from uri
    *
    * generates a url while attempting to maintain file extensions and
    * generation of index filename for uris ending in slash.
    * If uri includes query string it will generate an MD5 hash in the filename
    *
    * @param uri io.lemonlabs.uri.Uri
    * @param root root of path to generate
    * @param fileName optional filename which, if defined, will replace the last part of the uri
    * @return
    */
  def generateFilePath(uri: Uri, root: Option[String] = None, fileName: Option[String], contentType: Option[String]): String = {
    val targetDir = root.getOrElse("").replaceAll("/+$", "")

    val queryHash = if (uri.toUrl.query.isEmpty) "" else s".${
      MessageDigest.getInstance("MD5")
        .digest(uri.toUrl.query.toString.getBytes)
        .map(0xFF & _)
        .map { "%02x".format(_) }
        .foldLeft(""){_ + _}
    }"

    def insertQueryHash(pathEnd: String): String = {
      val hasExtension = """(.*)\.(.*)$""".r
      val headerExt = contentType.map(Adapter.contentTypeExtension).getOrElse(".html")

      pathEnd match {
        case "" ⇒ s"index$queryHash$headerExt"
        case hasExtension(p, ext) ⇒ s"$p$queryHash.$ext"
        case p ⇒ s"$p$queryHash$headerExt"
      }
    }

    def generateBasename(path: Path): String = {
      val allParts = path.parts ++ fileName.toVector
      val hashedLastPathPart = insertQueryHash(allParts.last)

      "/" + (allParts.init.filter(_.nonEmpty) ++ Vector(hashedLastPathPart)).mkString("/")
    }

    s"$targetDir/${
      uri.schemeOption match {
        case Some(scheme) ⇒ s"$scheme/"
        case None ⇒ ""
      }
    }${
      uri.toUrl.hostOption match {
        case Some(host) ⇒ s"$host"
        case None ⇒ ""
      }
    }${
      uri.path match {
        case EmptyPath ⇒ s"/index$queryHash.html"
        case path: RootlessPath ⇒ s"${generateBasename(path)}"
        case path: AbsolutePath ⇒ generateBasename(path)
        case _ ⇒ ""
      }
    }"
  }
}
