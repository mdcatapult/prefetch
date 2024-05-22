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

package io.mdcatapult.doclib.remote

import akka.stream.Materializer
import com.typesafe.config.Config
import io.lemonlabs.uri._
import io.mdcatapult.doclib.models.Origin
import io.mdcatapult.doclib.models.metadata.MetaInt
import io.mdcatapult.doclib.remote.adapters.{Ftp, Http}
import play.api.libs.ws.StandaloneWSRequest
import play.api.libs.ws.ahc._

import scala.concurrent.{ExecutionContext, Future}

class UnsupportedSchemeException(scheme: String) extends Exception(s"Scheme '$scheme' not currently supported")
class UndefinedSchemeException(uri: Uri) extends Exception(s"No scheme detected for ${uri.toString}")

class Client()(implicit config: Config, ec: ExecutionContext, m: Materializer) {

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
    headers = Some(response.headers.view.mapValues(_.toSeq).toMap),
    metadata = Some(List(MetaInt("status", response.status))))

  protected def makeOrigin(source: Uri): Origin = Origin(
    scheme = source.schemeOption.get,
    hostname = Some(source.toJavaURI.getHost),
    uri = Some(source),
    headers = None,
    metadata = None)

  def download(origin: Origin): Future[Option[DownloadResult]] = origin match {
    case origin if Http.protocols.contains(origin.uri.get.schemeOption.getOrElse("")) => {
      Http.download(origin)
    }
    case origin if Ftp.protocols.contains(origin.uri.get.schemeOption.getOrElse("")) => {
      Ftp.download(origin)
    }
    case _ => throw new UnsupportedSchemeException(origin.uri.get.schemeOption.getOrElse("unknown"))
  }

}
