package io.mdcatapult.doclib.remote

import akka.stream.Materializer
import com.typesafe.config.Config
import io.lemonlabs.uri._
import io.mdcatapult.doclib.models.Origin
import io.mdcatapult.doclib.models.metadata.MetaInt
import io.mdcatapult.doclib.remote.adapters.{Ftp, Http}
import io.mdcatapult.doclib.util.FileHash
import play.api.libs.ws.StandaloneWSRequest
import play.api.libs.ws.ahc._

import scala.concurrent.{ExecutionContext, Future}

class UnsupportedSchemeException(scheme: String) extends Exception(s"Scheme '$scheme' not currently supported")
class UndefinedSchemeException(uri: Uri) extends Exception(s"No scheme detected for ${uri.toString}")

class Client()(implicit config: Config, ec: ExecutionContext, m: Materializer) extends FileHash {

  /** initialise web client **/
  lazy val ahcwsCconfig: AhcWSClientConfig = AhcWSClientConfigFactory.forConfig(config)
  lazy val httpClient: StandaloneAhcWSClient = StandaloneAhcWSClient(AhcWSClientConfigFactory.forConfig(config))

  /**
    * does an initial check of a provided remote resource and returns a Resolved response
    *
    * @param source io.lemonlabs.uri.Uri
    * @return
    */
  def resolve(source: Uri): Future[List[Origin]] = source.schemeOption match {
      //TODO What should the metatdata be ("status": 200) ?
    case Some("http" | "https") => httpClient.url(source.toString).head().flatMap(response => {
        if (!List(301,302).contains(response.status)) {
          Future.successful(List(makeOrigin(response)))
        } else {
          resolve(Uri.parse(response.header("Location").getOrElse(throw new UnableToFollow(source.toStringRaw)))).map( redirectOrigin =>
            redirectOrigin ++ List(makeOrigin(response))
          )
        }
      })
    case Some("ftp" | "ftps" | "sftp") => Future.successful(List(makeOrigin(source)))
    case Some(unsupported) => throw new UnsupportedSchemeException(unsupported)
    case None => throw new UndefinedSchemeException(source)
  }

  protected def makeOrigin(response: StandaloneWSRequest#Response): Origin = Origin(
    scheme = response.uri.getScheme,
    hostname = Some(response.uri.getHost),
    uri = Some(Uri.parse(response.uri.toString)),
    headers = Some(response.headers),
    metadata = Some(List(MetaInt("status", response.status))))

  protected def makeOrigin(source: Uri): Origin = Origin(
    scheme = source.schemeOption.get,
    hostname = Some(source.toJavaURI.getHost),
    uri = Some(source),
    headers = None,
    metadata = None)

  def download(source: Uri): Option[DownloadResult] = source match {
    case Http(result: DownloadResult) => Some(result)
    case Ftp(result: DownloadResult) => Some(result)
    case _ => throw new UnsupportedSchemeException(source.schemeOption.getOrElse("unknown"))
  }

}
