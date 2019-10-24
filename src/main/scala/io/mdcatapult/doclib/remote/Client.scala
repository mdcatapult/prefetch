package io.mdcatapult.doclib.remote

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.typesafe.config.Config
import io.lemonlabs.uri._
import io.mdcatapult.doclib.models.Origin
import io.mdcatapult.doclib.models.metadata.MetaInt
import io.mdcatapult.doclib.remote.adapters.{Ftp, Http}
import io.mdcatapult.doclib.util.FileHash
import play.api.libs.ws.ahc._

import scala.concurrent.{ExecutionContextExecutor, Future}

class UnsupportedSchemeException(scheme: String) extends Exception(s"Scheme '$scheme' not currently supported")
class UndefinedSchemeException(uri: Uri) extends Exception(s"No scheme detected for ${uri.toString}")

class Client()(implicit config: Config, ex: ExecutionContextExecutor, system: ActorSystem, materializer: ActorMaterializer) extends FileHash {

  /** initialise web client **/
  lazy val httpClient = StandaloneAhcWSClient(AhcWSClientConfigFactory.forConfig(config))

  /**
    * does an initial check of a provided remote resource and returns a Resolved response
    *
    * @param source io.lemonlabs.uri.Uri
    * @return
    */
  def resolve(source: Uri): Future[Origin] = source.schemeOption match {
      //TODO What should the metatdata be ("status": 200) ?
    case Some("http" | "https") ⇒ httpClient.url(source.toString).head().map(r =>
      Origin(
        scheme = r.uri.getScheme,
        hostname = Some(r.uri.getHost),
        uri = Some(Uri.parse(r.uri.toString)),
        headers = Some(r.headers),
        metadata = Some(List(MetaInt("status", r.status))))
    )
    case Some("ftp" | "ftps" | "sftp") ⇒ Future.successful(Origin(
      scheme = source.schemeOption.get,
      hostname = Some(source.toJavaURI.getHost),
      uri = Some(source),
      headers = None,
      metadata = None))
    case Some(unsupported) ⇒ throw new UnsupportedSchemeException(unsupported)
    case None ⇒ throw new UndefinedSchemeException(source)
  }

  def download(source: Uri): Option[DownloadResult] = source match {
    case Http(result: DownloadResult) ⇒ Some(result)
    case Ftp(result: DownloadResult) ⇒ Some(result)
    case _ ⇒ throw new UnsupportedSchemeException(source.schemeOption.getOrElse("unknown"))
  }

}
