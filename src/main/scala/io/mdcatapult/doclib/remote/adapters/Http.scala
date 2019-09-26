package io.mdcatapult.doclib.remote.adapters

import java.io.File
import java.net.{HttpURLConnection, URL}

import com.typesafe.config.Config
import io.lemonlabs.uri.Uri
import io.mdcatapult.doclib.remote.DownloadResult
import io.mdcatapult.doclib.util.FileHash

import scala.sys.process._

case class Http(uri: Uri)

object Http extends Adapter with FileHash {

  val protocols = List("http", "https")

  def unapply(uri: Uri)(implicit config: Config): Option[DownloadResult] =
    if (protocols.contains(uri.schemeOption.getOrElse("")))
      Http.download(uri)
    else None

  /**
  * Fetches contents of URI using low level request and writes to file
  *
  * This method will not facilitate any form of Javascript rendering
  * Converts url to path using naive assumption that we are using linux
  * filesystem and does not attempt to convert of change the target path
  * for non linux filesystem
  *
  * @param uri io.lemonlabs.uri.Uri
  * @return
  */
  def download(uri: Uri)(implicit config: Config): Option[DownloadResult] = {
    val finalTarget = new File(generateFilePath(uri, Some(config.getString("prefetch.remote.target-dir"))))
    val tempTarget = new File(generateFilePath(uri, Some(config.getString("prefetch.remote.temp-dir"))))
    tempTarget.getParentFile.mkdirs()
    val url =  new URL(uri.toUrl.toString)
    try {
      val connection = url.openConnection().asInstanceOf[HttpURLConnection]
      connection.setConnectTimeout(5000)
      connection.setReadTimeout(5000)
      connection.connect()
      if (connection.getResponseCode >= 400) {
        None
        // TODO log errors somewhere
        // TODO Failure via Try may be better since upstream the error will be reported as an unhandled protocol
      } else {
        (new URL(uri.toUrl.toString) #> tempTarget).!!
        Some(DownloadResult(
          source = tempTarget.getAbsolutePath,
          hash = md5(tempTarget.getAbsolutePath),
          origin = Some(uri.toString),
          target = Some(finalTarget.getAbsolutePath)
        ))
      }
    } catch {
      case e: Exception => None
    }

  }
}
