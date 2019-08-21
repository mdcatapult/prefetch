package io.mdcatapult.doclib.remote.adapters

import java.io.File
import java.net.{InetAddress, URL}

import akka.actor.ActorSystem
import akka.stream.alpakka.ftp.FtpCredentials.AnonFtpCredentials
import akka.stream.alpakka.ftp.{FtpCredentials, FtpSettings, FtpsSettings, RemoteFileSettings, SftpSettings}
import com.typesafe.config.Config
import io.lemonlabs.uri.{Uri, Url}
import io.mdcatapult.doclib.remote.{DownloadResult, UndefinedSchemeException, UnsupportedSchemeException}
import io.mdcatapult.doclib.remote.adapters.Http.{generateFilePath, md5}
import io.mdcatapult.doclib.util.FileHash
import akka.stream.{ActorMaterializer, IOResult}
import akka.stream.alpakka.ftp.scaladsl.{Ftp ⇒ AkkaFtp, Ftps ⇒ AkkaFtps, Sftp ⇒ AkkaSftp}
import akka.stream.scaladsl.{FileIO, Sink, Source}
import akka.util.ByteString

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContextExecutor, Future}
import scala.util.{Failure, Success}
object Ftp extends Adapter with FileHash {

  val protocols = List("ftp", "ftps", "sftp")

  /**
    * test if supported scheme and perform download
    * @param uri Uri
    * @param config config
    * @return
    */
  def unapply(uri: Uri)(implicit config: Config): Option[DownloadResult] =
    if (protocols.contains(uri.schemeOption.getOrElse("")))
      Ftp.download(uri)
    else None

  /**
    * build FTP credentials for FTP connection
    * @param url Url
    * @return
    */
  protected def getFTPCredentials(url: Url): FtpCredentials = url.user match {
    case Some(user) ⇒ FtpCredentials.create(user, url.password.getOrElse(""))
    case None ⇒ AnonFtpCredentials
  }

  /**
    * build FTP config
    * @param url Url
    * @return
    */
  protected def getFtpSettings(url: Url): FtpSettings =
    FtpSettings
      .create(InetAddress.getByName(url.hostOption.get.toString()))
      .withPort(url.port.getOrElse(21))
      .withCredentials(getFTPCredentials(url))
      .withBinary(true)

  /**
    * build FTPS config
    * @param url Url
    * @return
    */
  protected def getFtpsSettings(url: Url): FtpsSettings =
    FtpsSettings
      .create(InetAddress.getByName(url.hostOption.get.toString()))
      .withPort(url.port.getOrElse(21))
      .withCredentials(getFTPCredentials(url))
      .withBinary(true)

  /**
    * Build SFTP config
    * @param url Url
    * @return
    */
  protected def getSftpSettings(url: Url): SftpSettings =
    SftpSettings
      .create(InetAddress.getByName(url.hostOption.get.toString()))
      .withPort(url.port.getOrElse(22))
      .withCredentials(getFTPCredentials(url))
      .withStrictHostKeyChecking(false)

  /**
    * retrieve a file from the ftp source
    * @param url Url
    * @return
    */
  protected def retrieve(url: Url): Source[ByteString, Future[IOResult]] = url.schemeOption match  {
    case Some("ftp")  ⇒ AkkaFtp.fromPath(url.path.toString(), getFtpSettings(url))
    case Some("ftps") ⇒ AkkaFtps.fromPath(url.path.toString(), getFtpsSettings(url))
    case Some("sftp") ⇒ AkkaSftp.fromPath(url.path.toString(), getSftpSettings(url))
    case Some(unknown) ⇒ throw new UnsupportedSchemeException(unknown)
    case  None ⇒ throw new UndefinedSchemeException(url)
  }

  /**
    * download a file from the ftp server and store it locally returning a DownloadResult
    * @param source Uri
    * @param config Config
    * @return
    */
  def download(source: Uri)(implicit config: Config): Option[DownloadResult] = {
    implicit val system: ActorSystem = ActorSystem("consumer-prefetch-ftp", config)
    implicit val materializer: ActorMaterializer = ActorMaterializer()
    implicit val executor: ExecutionContextExecutor = system.dispatcher

    val finalTarget = getTargetPath(source)
    val tempTarget = getTempPath((source))
    tempTarget.getParentFile.mkdirs()
    val r: Future[Some[DownloadResult]] = retrieve(source.toUrl)
      .runWith(FileIO.toPath(tempTarget.toPath))
      .map({ioresult: IOResult ⇒ ioresult.status match {
        case Success(_) ⇒ Some(DownloadResult(
          source = tempTarget.getAbsolutePath,
          hash = md5(tempTarget.getAbsolutePath),
          origin = Some(source.toString),
          target = Some(finalTarget.getAbsolutePath)
        ))
        case Failure(exception) ⇒ throw exception

      }})
    Await.result(system.terminate(), Duration.Inf)
    Await.result(r, Duration.Inf)
  }
}
