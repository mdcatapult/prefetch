package io.mdcatapult.doclib.remote.adapters

import java.security.MessageDigest

import com.typesafe.config.Config
import io.lemonlabs.uri.{AbsolutePath, EmptyPath, Path, RootlessPath, Uri}
import io.mdcatapult.doclib.remote.DownloadResult

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
    * @return
    */
  def generateFilePath(uri: Uri, root: Option[String] = None): String = {
    val targetDir = root.getOrElse("").replaceAll("/+$", "")

    val queryHash = if (uri.toUrl.query.isEmpty) "" else s".${
      MessageDigest.getInstance("MD5").digest(uri.toUrl.query.toString.getBytes)
    }"

    def generateBasename(path: Path) = {
      val endsWithSlash = """(.*/)$""".r
      val hasExtension = """(.*)\.(.*)$""".r
      path.toString match {
        case endsWithSlash(p) ⇒ s"${p}index$queryHash.html"
        case hasExtension(p, ext) ⇒ s"$p$queryHash.$ext"
        case p ⇒ s"$p$queryHash.html"
      }
    }

    s"$targetDir/${
      uri.schemeOption match {
        case Some(scheme) ⇒ s"$scheme/"
        case None ⇒ ""
      }
    }${
      uri.toUrl.hostOption match {
        case Some(host) ⇒ s"$host/"
        case None ⇒ ""
      }
    }${
      uri.path match {
        case EmptyPath ⇒ s"/index$queryHash.html" // assumes http url
        case path: RootlessPath ⇒ s"/${generateBasename(path)}"
        case path: AbsolutePath ⇒ generateBasename(path)
        case _ ⇒ ""
      }
    }"
  }
}
