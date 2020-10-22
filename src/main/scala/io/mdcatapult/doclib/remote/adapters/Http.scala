package io.mdcatapult.doclib.remote.adapters

import java.io.File
import java.nio.file.Paths

import akka.actor.ActorSystem
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.`Set-Cookie`
import akka.http.scaladsl.{Http => AkkaHttp}
import akka.stream.scaladsl.FileIO
import akka.stream.{IOResult, Materializer, StreamTcpException}
import better.files.{File => ScalaFile}
import com.typesafe.config.Config
import io.lemonlabs.uri.Uri
import io.mdcatapult.doclib.models.Origin
import io.mdcatapult.doclib.remote.{DownloadResult, UnableToFollow, UndefinedSchemeException, UnsupportedSchemeException}
import io.mdcatapult.doclib.util.FileHash.hashOrOriginal
import io.mdcatapult.doclib.util.Metrics._
import io.mdcatapult.doclib.util.MimeType
import io.mdcatapult.util.hash.Md5.md5

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

object Http extends Adapter {

  val protocols = List("http", "https")

  val maxRedirection = 20

  case class Result(fileName: Option[String], contentType: Option[String], entity: HttpEntity)

  def unapply(origin: Origin)(implicit config: Config, m: Materializer): Option[DownloadResult] =
    if (protocols.contains(origin.uri.get.schemeOption.getOrElse("")))
      Http.download(origin)
    else
      None

  /**
   * Create appropriate https download mechanism for file
   * @param uri Resolved location of remote file
   * @return
   */
  protected def retrieve(uri: Uri)(implicit system: ActorSystem): Future[Result] =
    retrieve(uri, CookieJar.empty, Nil)

  private def retrieve(uri: Uri, cookieJar: CookieJar, redirections: List[Uri])(implicit system: ActorSystem): Future[Result] = {

    import system.dispatcher

    if (redirections.size == maxRedirection)
      throw new TooManyRedirectsException(redirections.reverse)

    uri.schemeOption match {
      case Some(x) if protocols.contains(x) =>
        AkkaHttp().singleRequest(
          HttpRequest(
            uri = uri.toString,
            headers = cookieJar.getCookies(uri)
          )
        ).flatMap {
          case HttpResponse(StatusCodes.OK, headers, entity, _) =>
            Future.successful(Result(Headers.filename(headers), Headers.contentType(headers), entity))
          case resp @ HttpResponse(status, _, _, _) if status.isRedirection() =>
            resp.discardEntityBytes()
            val location = resp.headers.find(_.lowercaseName == "location")

            location match {
              case Some(x) =>
                retrieve(
                  Uri.parse(x.value),
                  cookieJar.addCookies(uri, resp.headers[`Set-Cookie`]),
                  uri :: redirections)
              case None => throw new UnableToFollow(x)
            }
          case resp @ HttpResponse(status, _, _, _) =>
            resp.discardEntityBytes()
            throw new Exception(s"Unable to process $uri with status code $status")
        }
      case Some(unknown) => throw new UnsupportedSchemeException(unknown)
      case None => throw new UndefinedSchemeException(uri)
    }
  }

  /**
   * Fetches contents of URI using akka http and writes to file
   *
   * This method will not facilitate any form of Javascript rendering
   * Converts url to path using naive assumption that we are using linux
   * filesystem and does not attempt to convert or change the target path
   * for non linux filesystem
   *
   * @param origin io.mdcatapult.doclib.models.Origin
   * @return
   */
  def download(origin: Origin)(implicit config: Config, m: Materializer): Option[DownloadResult] = {
    //TODO We should probably turn this all into futures and for-comps but is a bigger refactor
    // and something for another issue.
    implicit val system: ActorSystem = ActorSystem("consumer-prefetch-http", config)
    import system.dispatcher

    val doclibRoot = config.getString("doclib.root")
    val uri = origin.uri.get
    val latency = documentFetchLatency.labels("http").startTimer()

    Await.result(retrieve(uri).recover {
      // Something happened before fetching file, might want to do something about it....
      case streamException: StreamTcpException => throw streamException
      case e: UnableToFollow => throw e
      case e: Exception => throw DoclibHttpRetrievalError(e.getMessage, e)
    } flatMap { x: Result =>

      val remotePath = generateFilePath(origin, Some(config.getString("doclib.remote.target-dir")), x.fileName, x.contentType)
      val tempPath = generateFilePath(origin, Some(config.getString("doclib.remote.temp-dir")), x.fileName, x.contentType)

      val finalTarget = Paths.get(s"$doclibRoot/$remotePath").toFile
      val tempTarget = Paths.get(s"$doclibRoot/$tempPath").toFile

      val (tempPathFinal: String, tempTargetFinal: String, finalTargetFinal: String) = hashOrOriginal(uri, ScalaFile(tempPath).name) match {
        case orig if orig == tempTarget.getName => (tempPath, tempTarget.toString, finalTarget.toString)
        case hashed => (tempPath.replace(tempTarget.getName, hashed), tempTarget.toString.replace(tempTarget.getName, hashed), finalTarget.toString.replace(finalTarget.getName, hashed))
      }

      tempTarget.getParentFile.mkdirs()

      val r: Future[IOResult] =
        x.entity.dataBytes.runWith(FileIO.toPath(tempTarget.toPath)).recover {
          // Something happened before fetching file, might want to do something about it....
          case e: Exception =>
            latency.observeDuration()
            throw DoclibHttpRetrievalError(e.getMessage, e)
        }

      latency.observeDuration()
      documentSizeBytes.labels("http", MimeType.getMimetype(finalTargetFinal)).observe(new File(tempTargetFinal).length().toDouble)
      r.map(_ =>
        Some(DownloadResult(
          source = tempPathFinal,
          hash = md5(new File(tempTargetFinal)),
          origin = Some(uri.toString),
          target = Some(new File(finalTargetFinal).getAbsolutePath)
        ))
      )
    }, Duration.Inf)
  }
}
