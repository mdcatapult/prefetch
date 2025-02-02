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

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.model._
import org.apache.pekko.http.scaladsl.model.headers.`Set-Cookie`
import org.apache.pekko.http.scaladsl.{Http => AkkaHttp}
import org.apache.pekko.stream.scaladsl.FileIO
import org.apache.pekko.stream.{IOResult, Materializer, StreamTcpException}
import better.files.{File => ScalaFile}
import com.typesafe.config.Config
import io.lemonlabs.uri.Uri
import io.mdcatapult.doclib.models.Origin
import io.mdcatapult.doclib.remote.{DownloadResult, UnableToFollow, UndefinedSchemeException, UnsupportedSchemeException}
import io.mdcatapult.doclib.util.FileHash.hashOrOriginal
import io.mdcatapult.doclib.util.Metrics._
import io.mdcatapult.doclib.util.MimeType
import io.mdcatapult.util.hash.Md5.md5

import java.io.File
import java.nio.file.Paths
import scala.concurrent.Future

object Http extends Adapter {

  val protocols = List("http", "https")

  val maxRedirection = 20

  case class Result(fileName: Option[String], contentType: Option[String], entity: HttpEntity)

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
          case resp @ HttpResponse(status, _, _, _) =>
            if (status.isRedirection()) {
              resp.discardEntityBytes()
              val location = resp.headers.find(_.lowercaseName == "location")

              location match {
                case Some(x) =>
                  retrieve(
                    Uri.parse(x.value),
                    cookieJar.addCookies(uri, resp.headers[`Set-Cookie`]),
                    uri :: redirections)
                case None => throw new UnableToFollow("Expected location header not found")
              }
            } else {
              throw new Exception(s"Unable to process $uri with status code $status")
            }
          case _ => throw new Exception(s"Unable to process $uri. Error is unknown")
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
  def download(origin: Origin)(implicit config: Config, m: Materializer): Future[Option[DownloadResult]] = {
    //TODO We should probably turn this all into futures and for-comps but is a bigger refactor
    // and something for another issue.
    implicit val system: ActorSystem = ActorSystem("consumer-prefetch-http", config)
    import system.dispatcher

    val doclibRoot = config.getString("doclib.root")
    val uri = origin.uri.get
    val latency = documentFetchLatency.labels("http").startTimer()

    retrieve(uri).recover {
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

      r.map(_ => {
        latency.observeDuration()
        documentSizeBytes.labels("http", MimeType.getMimetype(tempTargetFinal)).observe(new File(tempTargetFinal).length().toDouble)
        Some(DownloadResult(
          source = tempPathFinal,
          hash = md5(new File(tempTargetFinal)),
          origin = Some(uri.toString),
          target = Some(new File(finalTargetFinal).getAbsolutePath)
        ))
      })
    }
  }

}
