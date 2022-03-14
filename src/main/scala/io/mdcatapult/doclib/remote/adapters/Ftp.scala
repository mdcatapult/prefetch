package io.mdcatapult.doclib.remote.adapters

import akka.actor.ActorSystem
import akka.stream.alpakka.ftp.FtpCredentials.AnonFtpCredentials
import akka.stream.alpakka.ftp.scaladsl.{Ftp => AkkaFtp, Ftps => AkkaFtps, Sftp => AkkaSftp}
import akka.stream.alpakka.ftp.{FtpCredentials, FtpSettings, FtpsSettings, SftpSettings}
import akka.stream.scaladsl.{FileIO, Source}
import akka.stream.{IOResult, Materializer}
import akka.util.ByteString
import com.typesafe.config.Config
import io.lemonlabs.uri.Url
import io.mdcatapult.doclib.models.Origin
import io.mdcatapult.doclib.remote.{DownloadResult, UndefinedSchemeException, UnsupportedSchemeException}
import io.mdcatapult.doclib.util.Metrics._
import io.mdcatapult.doclib.util.MimeType
import io.mdcatapult.util.hash.Md5.md5

import java.net.InetAddress
import java.nio.file.Paths
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.{Failure, Success, Try}

object Ftp extends Adapter {

  val protocols = List("ftp", "ftps", "sftp")

  /**
   * build FTP credentials for FTP connection
   *
   * @param url Url
   * @return
   */
  protected def getFTPCredentials(url: Url): FtpCredentials = url.user match {
    case Some(user) => FtpCredentials.create(user, url.password.getOrElse(""))
    case None => AnonFtpCredentials
  }

  /**
   * build FTP config
   *
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
   *
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
   *
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
   *
   * @param url Url
   * @return
   */
  protected def retrieve(url: Url): Try[Source[ByteString, Future[IOResult]]] = {
    // The Akka from Path calls can throw an exception so Try it so that it gets handled
    // in the Future.
    Try {
      url.schemeOption match {
        case Some("ftp") => AkkaFtp.fromPath(url.path.toString, getFtpSettings(url))
        case Some("ftps") => AkkaFtps.fromPath(url.path.toString, getFtpsSettings(url))
        case Some("sftp") => AkkaSftp.fromPath(url.path.toString, getSftpSettings(url))
        case Some(unknown) => throw new UnsupportedSchemeException(unknown)
        case None => throw new UndefinedSchemeException(url)
      }
    }
  }

  /**
   * download a file from the ftp server and store it locally returning a DownloadResult
   *
   * @param origin io.mdcatapult.doclib.models.Origin
   * @param config Config
   * @return
   */
  def download(origin: Origin)(implicit config: Config, m: Materializer): Future[Option[DownloadResult]] = {
    implicit val system: ActorSystem = ActorSystem("consumer-prefetch-ftp", config)
    implicit val executor: ExecutionContextExecutor = system.dispatcher

    val doclibRoot = config.getString("doclib.root")
    val remotePath = generateFilePath(origin, Option(config.getString("doclib.remote.target-dir")), None, None)
    val tempPath = generateFilePath(origin, Option(config.getString("doclib.remote.temp-dir")), None, None)
    val finalTarget = Paths.get(s"$doclibRoot/$remotePath").toFile
    val tempTarget = Paths.get(s"$doclibRoot/$tempPath").toFile
    val latency = documentFetchLatency.labels("ftp").startTimer()
    tempTarget.getParentFile.mkdirs()
    val r: Future[IOResult] = retrieve(origin.uri.get.toUrl).map(x =>

      x.runWith(FileIO.toPath(tempTarget.toPath.toAbsolutePath)).recover {
        // Something happened before fetching file, might want to do something about it....
        case e: Exception =>
          latency.observeDuration()
          throw DoclibFtpRetrievalError(e.getMessage, e)
      }
    ) match {
      case Success(value) => value
      case Failure(exception) => Future.failed(exception)
    }

    r.map(_ => {
      latency.observeDuration()
      documentSizeBytes.labels("ftp", MimeType.getMimetype(tempTarget.getAbsolutePath)).observe(tempTarget.length().toDouble)
      Some(DownloadResult(
        source = tempPath,
        hash = md5(tempTarget.getAbsoluteFile),
        origin = Option(origin.uri.get.toString),
        target = Option(finalTarget.getAbsolutePath)
      ))
    }
    )
  }

}
