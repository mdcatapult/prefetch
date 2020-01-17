package io.mdcatapult.doclib.remote.adapters

import java.io.File
import java.nio.file.Paths

import akka.actor.ActorSystem
import akka.http.scaladsl.model._
import akka.http.scaladsl.{Http ⇒ AkkaHttp}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.FileIO
import com.typesafe.config.Config
import io.lemonlabs.uri.Uri
import io.mdcatapult.doclib.remote.{DownloadResult, UndefinedSchemeException, UnsupportedSchemeException}
import io.mdcatapult.doclib.util.FileHash

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContextExecutor, Future}

case class Http(uri: Uri)

object Http extends Adapter with FileHash {

  val protocols = List("http", "https")

  def unapply(uri: Uri)(implicit config: Config): Option[DownloadResult] =
    if (protocols.contains(uri.schemeOption.getOrElse("")))
      Http.download(uri)
    else None

  /**
   * Create appropriate https download mechanism for file
   * @param uri Resolved location of remote file
   * @return
   */
  protected def retrieve(uri: Uri)(implicit actor: ActorSystem): Future[HttpResponse] = uri.schemeOption match  {
    case Some("http") | Some("https") ⇒ AkkaHttp().singleRequest(HttpRequest(uri = uri.toString()))
    case Some(unknown) ⇒ throw new UnsupportedSchemeException(unknown)
    case  None ⇒ throw new UndefinedSchemeException(uri)
  }

  /**
   * Fetches contents of URI using low level request and writes to file
   *
   * This method will not facilitate any form of Javascript rendering
   * Converts url to path using naive assumption that we are using linux
   * filesystem and does not attempt to convert or change the target path
   * for non linux filesystem
   *
   * @param uri io.lemonlabs.uri.Uri
   * @return
   */
  def download(uri: Uri)(implicit config: Config): Option[DownloadResult] = {
    implicit val system: ActorSystem = ActorSystem("consumer-prefetch-ftp", config)
    implicit val materializer: ActorMaterializer = ActorMaterializer()
    implicit val executor: ExecutionContextExecutor = system.dispatcher
    val doclibRoot = config.getString("doclib.root")
    val remotePath = generateFilePath(uri, Some(config.getString("doclib.remote.target-dir")))
    val tempPath = generateFilePath(uri, Some(config.getString("doclib.remote.temp-dir")))
    val finalTarget = new File(Paths.get(s"$doclibRoot/$remotePath").toString)
    val tempTarget = new File(Paths.get(s"$doclibRoot/$tempPath").toString)

    tempTarget.getParentFile.mkdirs()

    val responseFuture: Future[HttpResponse] = retrieve(uri)
    val response = responseFuture.map {
      case HttpResponse(StatusCodes.OK, headers, entity, _) ⇒ {
        entity.dataBytes.runWith(FileIO.toPath(tempTarget.toPath))
        Some(DownloadResult(
          source = tempPath,
          hash = md5(tempTarget.getAbsolutePath),
          origin = Some(uri.toString),
          target = Some(finalTarget.getAbsolutePath)
        ))
      }
      case resp @ HttpResponse(status, _, _, _) ⇒ {
        resp.discardEntityBytes()
        throw new Exception(s"Unable to process $uri with status code $status")
      }
    }
    // TODO Not sure we should really wait for ever but how long?
    Await.result(response, Duration.Inf)
  }
}