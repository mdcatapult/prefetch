package io.mdcatapult.doclib.remote.adapters

import java.net.InetAddress
import java.nio.file.Paths

import akka.actor.ActorSystem
import akka.stream.alpakka.ftp.FtpCredentials.AnonFtpCredentials
import akka.stream.alpakka.ftp.scaladsl.{Ftp => AkkaFtp, Ftps => AkkaFtps, Sftp => AkkaSftp}
import akka.stream.alpakka.ftp.{FtpCredentials, FtpSettings, FtpsSettings, SftpSettings}
import akka.stream.scaladsl.{FileIO, Source}
import akka.stream.{IOResult, Materializer}
import akka.util.ByteString
import com.typesafe.config.Config
import io.lemonlabs.uri.{Uri, Url}
import io.mdcatapult.doclib.remote.{DownloadResult, UndefinedSchemeException, UnsupportedSchemeException}
import io.mdcatapult.doclib.util.FileHash
import io.mdcatapult.doclib.util.HashUtils.md5

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContextExecutor, Future}

object Ftp extends Adapter with FileHash {

  val protocols = List("ftp", "ftps", "sftp")

  /**
    * test if supported scheme and perform download
    * @param uri Uri
    * @param config config
    * @return
    */
  def unapply(uri: Uri)(implicit config: Config, m: Materializer): Option[DownloadResult] =
    if (protocols.contains(uri.schemeOption.getOrElse("")))
      Ftp.download(uri)
    else None

  /**
    * build FTP credentials for FTP connection
    * @param url Url
    * @return
    */
  protected def getFTPCredentials(url: Url): FtpCredentials = url.user match {
    case Some(user) => FtpCredentials.create(user, url.password.getOrElse(""))
    case None => AnonFtpCredentials
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
      .withPassiveMode(true)

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
      .withPassiveMode(true)

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
    case Some("ftp")  => AkkaFtp.fromPath(url.path.toString(), getFtpSettings(url))
    case Some("ftps") => AkkaFtps.fromPath(url.path.toString(), getFtpsSettings(url))
    case Some("sftp") => AkkaSftp.fromPath(url.path.toString(), getSftpSettings(url))
    case Some(unknown) => throw new UnsupportedSchemeException(unknown)
    case None => throw new UndefinedSchemeException(url)
  }

  /**
    * download a file from the ftp server and store it locally returning a DownloadResult
    * @param uri Uri
    * @param config Config
    * @return
    */
  def download(uri: Uri)(implicit config: Config, m: Materializer): Option[DownloadResult] = {
    implicit val system: ActorSystem = ActorSystem("consumer-prefetch-ftp", config)
    implicit val executor: ExecutionContextExecutor = system.dispatcher

    val doclibRoot = config.getString("doclib.root")
    val remotePath = generateFilePath(uri, Option(config.getString("doclib.remote.target-dir")), None, None)
    val tempPath = generateFilePath(uri, Option(config.getString("doclib.remote.temp-dir")), None, None)
    val finalTarget = Paths.get(s"$doclibRoot/$remotePath").toFile
    val tempTarget = Paths.get(s"$doclibRoot/$tempPath").toFile

    tempTarget.getParentFile.mkdirs()
    val r: Future[IOResult] = retrieve(uri.toUrl)
      .runWith(FileIO.toPath(tempTarget.toPath.toAbsolutePath))

    val a = r.map(_ => Some(DownloadResult(
          source = tempPath,
          hash = md5(tempTarget.getAbsoluteFile),
          origin = Option(uri.toString),
          target = Option(finalTarget.getAbsolutePath)
        ))
      )
    Await.result(a, Duration.Inf)
  }
}
