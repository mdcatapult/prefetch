package io.mdcatapult.doclib.remote.adapters

import java.io.File
import java.net.URL

import com.typesafe.config.Config
import io.lemonlabs.uri.Uri
import io.mdcatapult.doclib.remote.DownloadResult
import io.mdcatapult.doclib.util.FileHash
import scala.sys.process._

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
    (new URL(uri.toUrl.toString) #> tempTarget).!!

    Some(DownloadResult(
      source = tempTarget.getAbsolutePath,
      hash = md5(tempTarget.getAbsolutePath),
      origin = Some(uri.toString),
      target = Some(finalTarget.getAbsolutePath)
    ))
  }
}
