package io.mdcatapult.doclib.remote.adapters

import java.io.File
import java.nio.file.Paths

import akka.actor.ActorSystem
import akka.http.scaladsl.model._
import akka.http.scaladsl.{Http ⇒ AkkaHttp}
import akka.stream.scaladsl.FileIO
import akka.stream.{ActorMaterializer, IOResult, StreamTcpException}
import com.typesafe.config.Config
import better.files.{File ⇒ ScalaFile, _}
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
   * Fetches contents of URI using akka http and writes to file
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
    //TODO We should probably turn this all into futures and for-comps but is a bigger refactor
    // and something for another issue.
    implicit val system: ActorSystem = ActorSystem("consumer-prefetch-http", config)
    implicit val materializer: ActorMaterializer = ActorMaterializer()
    implicit val executor: ExecutionContextExecutor = system.dispatcher
    val doclibRoot = config.getString("doclib.root")
    val remotePath = generateFilePath(uri, Some(config.getString("doclib.remote.target-dir")))
    val tempPath = generateFilePath(uri, Some(config.getString("doclib.remote.temp-dir")))
    val finalTarget = new File(Paths.get(s"$doclibRoot/$remotePath").toString)
    val tempTarget = new File(Paths.get(s"$doclibRoot/$tempPath").toString)
    val (tempPathFinal: String, tempTargetFinal: String, finalTargetFinal: String) = hashOrOriginal(uri, ScalaFile(tempPath).name) match {
      case orig if orig == tempTarget.getName ⇒ (tempPath, tempTarget.toString, finalTarget.toString)
      case hashed ⇒ (tempPath.replace(tempTarget.getName, hashed), tempTarget.toString.replace(tempTarget.getName, hashed), finalTarget.toString.replace(finalTarget.getName, hashed))
    }
    tempTarget.getParentFile.mkdirs()

    // Get the http response
    val responseFuture: Future[HttpResponse] = retrieve(uri).recover {
      // Something happened before fetching file, might want to do something about it....
      case streamException: StreamTcpException => throw streamException
      case e: Exception ⇒ throw e
    }
    val response = responseFuture.map {
      case HttpResponse(StatusCodes.OK, headers, entity, _) ⇒ {
        entity
      }
      case resp @ HttpResponse(status, _, _, _) ⇒ {
        resp.discardEntityBytes()
        throw new Exception(s"Unable to process $uri with status code $status")
      }
    }
    // TODO Not sure we should really wait for ever but how long?
    val entity = Await.result(response, Duration.Inf)
    // We have the response entity so write it to file
    val finishedWriting = entity.dataBytes.runWith(FileIO.toPath(tempTarget.toPath)).recover {
      // Something happened writing the file, might want to do something with it
      case e: Exception ⇒ throw e
    }
    Await.result(finishedWriting, Duration.Inf) match {
      case IOResult(count, status) ⇒
        Some(DownloadResult(
          source = tempPathFinal,
          hash = md5(new File(tempTargetFinal).getAbsolutePath),
          origin = Some(uri.toString),
          target = Some(new File(finalTargetFinal).getAbsolutePath)
        ))
    }
  }
}