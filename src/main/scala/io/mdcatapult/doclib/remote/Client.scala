package io.mdcatapult.doclib.remote

import java.io.File
import java.net.URL
import java.security.MessageDigest

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.typesafe.config.Config
import io.lemonlabs.uri._
import io.mdcatapult.doclib.models.PrefetchOrigin
import play.api.libs.ws.ahc._

import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.sys.process._

class UnsupportedSchemeException(scheme: String) extends Exception(s"Scheme '$scheme' not currently supported")
class UndefinedSchemeException(uri: Uri) extends Exception(s"No scheme detected for ${uri.toString}")

class Client()(implicit config: Config, ex: ExecutionContextExecutor, system: ActorSystem, materializer: ActorMaterializer) {

  /** initialise web client **/
  lazy val httpClient = StandaloneAhcWSClient()

  /**
    * does an initial check of a provided remote resource and returns a Resolved response
    *
    * @param source io.lemonlabs.uri.Uri
    * @return
    */
  def resolve(source: Uri): Future[PrefetchOrigin] = source.schemeOption match {
    case Some("http" | "https") ⇒ httpClient.url(source.toString).head().map(r =>
      PrefetchOrigin(
        scheme = r.uri.getScheme,
        uri = Some(Uri.parse(r.uri.toString)),
        headers = Some(r.headers),
        metadata = Some(Map[String, Any]("status" → r.status, "statusText" → r.statusText)))
    )
    case Some(unsupported) ⇒ throw new UnsupportedSchemeException(unsupported)
    case None ⇒ throw new UndefinedSchemeException(source)
  }

  def download(source: Uri): Option[DownloadResult] = source.schemeOption match {
    case Some("http" | "https") ⇒ downloadHttp(source)
    case Some(unsupported) ⇒ throw new UnsupportedSchemeException(unsupported)
    case None ⇒ throw new UndefinedSchemeException(source)
  }

  /**
    * Fetches contents of URI using low level request and writes to file
    *
    * This method will not facilitate any form of Javascript rendering
    * Converts url to path using naive assumption that we are using linux
    * filesystem and does not attempt to convert of change the target path
    * for non linux filesystem
    *
    * @param source io.lemonlabs.uri.Uri
    * @return
    */
  def downloadHttp(source: Uri): Option[DownloadResult] = {
    val target = new File(generateFilePath(source))
    target.getParentFile.mkdirs()
    (new URL(source.toUrl.toString) #> target).!!
    Some(DownloadResult(source = target.getAbsolutePath, origin = Some(source.toString)))
  }

  /**
    * generate path on filesystem from uri
    *
    * generates a url while attempting to maintain file extensions and
    * generation of index filename for uris ending in slash.
    * If uri includes query string it will generate an MD5 hash in the filename
    *
    * @param uri io.lemonlabs.uri.Uri
    * @return
    */
  def generateFilePath(uri: Uri): String = {
    val targetDir = config.getString("prefetch.remote.target-dir").replaceAll("/+$", "")

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
