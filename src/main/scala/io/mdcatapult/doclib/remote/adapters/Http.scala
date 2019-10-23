package io.mdcatapult.doclib.remote.adapters

import java.io.File
import java.net.URL
import java.nio.file.Paths

import com.typesafe.config.Config
import io.lemonlabs.uri.Uri
import io.mdcatapult.doclib.remote.DownloadResult
import io.mdcatapult.doclib.util.FileHash
import better.files.{File ⇒ ScalaFile, _}
import better.files.Dsl._

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
  * for non linux filesystem.
  * If the file name is too long then replace with a hash of the original name
  *
  * @param uri io.lemonlabs.uri.Uri
  * @return
  */
  def download(uri: Uri)(implicit config: Config): Option[DownloadResult] = {
    val doclibRoot = config.getString("doclib.root")
    val remotePath = generateFilePath(uri, Some(config.getString("doclib.remote.target-dir")))
    val tempPath = generateFilePath(uri, Some(config.getString("doclib.remote.temp-dir")))
    val finalTarget = new File(Paths.get(s"$doclibRoot/$remotePath").toString)
    val tempTarget = new File(Paths.get(s"$doclibRoot/$tempPath").toString)
    val (tempTargetFinal: String, finalTargetFinal: String) = hashOrOriginal(uri, ScalaFile(tempPath).name) match {
      case orig if orig == tempTarget.getName ⇒ (tempTarget.toString, finalTarget.toString)
      case hashed ⇒ (tempTarget.toString.replace(tempTarget.getName, hashed), finalTarget.toString.replace(finalTarget.getName, hashed))
    }
    mkdirs (ScalaFile(tempTargetFinal).parent)
    val validStatusRegex = """HTTP/[0-9]\.[0-9]\s([0-9]{3})\s(.*)""".r
    val url =  new URL(uri.toUrl.toString)
    url.openConnection().getHeaderField(null) match {
      case validStatusRegex(code, message) ⇒ code.toInt match {
        case c if c < 400 ⇒
          (url #> new File(tempTargetFinal)).!!
          Some(DownloadResult(
            source = tempTargetFinal.replaceFirst(s"$doclibRoot/", ""),
            hash = md5(new File(tempTargetFinal).getAbsolutePath),
            origin = Some(uri.toString),
            target = Some(new File(finalTargetFinal).getAbsolutePath)
          ))
        case _ ⇒ throw new Exception(s"Unable to process URL with resolved status code of $code")
      }
      case _ ⇒ throw new Exception(s"Unable to retrieve headers for URL ${url.toString}")
    }
  }
}
