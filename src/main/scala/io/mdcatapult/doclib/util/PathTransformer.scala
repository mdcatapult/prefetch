package io.mdcatapult.doclib.util

import com.typesafe.config.Config
import io.mdcatapult.doclib.handlers.FoundDoc
import io.mdcatapult.doclib.models.Origin
import io.mdcatapult.doclib.path.TargetPath
import io.mdcatapult.doclib.remote.adapters.Http
import better.files._

import java.nio.file.{Files, Paths}
import java.nio.file.attribute.BasicFileAttributeView

/**
 * Util methods to figure out local & remote file paths within the doclib root
 */
trait PathTransformer extends TargetPath {

  implicit val config: Config

  val sharedConfig = FileConfig.getSharedConfig(config)
  val doclibRoot = sharedConfig.doclibRoot
  val localDirName = sharedConfig.localDirName
  val remoteDirName = sharedConfig.remoteDirName
  val archiveDirName = sharedConfig.archiveDirName

  /**
   * Tests if found Document currently in the remote root and if not returns the appropriate download target required
   *
   * @param foundDoc Found Document and remote data
   * @return
   */
  def getRemoteUpdateTargetPath(foundDoc: FoundDoc): Option[String] =
    if (inRemoteRoot(foundDoc.doc.source))
      Some(Paths.get(s"${foundDoc.doc.source}").toString)
    else
      Some(Paths.get(s"${foundDoc.download.get.target.get}").toString.replaceFirst(s"^$doclibRoot/*", ""))

  /**
   * Determines appropriate local target path. Often because we are moving from
   * ingress to local. ie the file is being ingested from the file system
   *
   * @param foundDoc Found Doc
   * @return
   */
  def getLocalUpdateTargetPath(foundDoc: FoundDoc): Option[String] =
    if (inLocalRoot(foundDoc.doc.source))
      Some(Paths.get(s"${foundDoc.doc.source}").toString)
    else {
      // strips temp dir if present plus any prefixed slashes
      val relPath = foundDoc.doc.source.replaceFirst(s"^$doclibRoot/*", "")
      Some(Paths.get(getTargetPath(relPath, localDirName)).toString)
    }

  /**
   * Wraps a method that matches the required signature of FoundDoc => Option[String] because
   * we need the Origin for this path transform. It moves a "remote" file ie one that in reality
   * came from an http/ftp origin but we want to ingest it via the file system and placed it in
   * the doclib local folder
   * @param origin
   * @return
   */
  def getLocalToRemoteTargetUpdatePath(origin: Origin): FoundDoc => Option[String] = {
    def getTargetPath(foundDoc: FoundDoc): Option[String] =
      if (inRemoteRoot(foundDoc.doc.source))
        Some(Paths.get(s"${foundDoc.doc.source}").toString)
      else {
        val remotePath = Http.generateFilePath(origin, Option(remoteDirName), None, None)
        Some(Paths.get(s"$remotePath").toString)
      }

    getTargetPath
  }

  /**
   * generate an archive for the found document
   *
   * @param targetPath the found doc
   * @return
   */
  def getArchivePath(targetPath: String, hash: String): String = {
    // withExt will incorrectly match files without extensions if there is a "." in the path.
    val withExt = """^(.+)/([^/]+)\.(.+)$""".r
    val withoutExt = """^(.+)/([^/\.]+)$""".r

    // match against withoutExt first and fall through to withExt
    targetPath match {
      case withoutExt(path, file) => s"${getTargetPath(path, archiveDirName)}/$file/$hash"
      case withExt(path, file, ext) => s"${getTargetPath(path, archiveDirName)}/$file.$ext/$hash.$ext"
      case _ => throw new RuntimeException(s"Unable to identify path and filename for targetPath: $targetPath")
    }
  }

  def zeroLength(filePath: String): Boolean = {
    val absPath = (doclibRoot / filePath).path
    val attrs = Files.getFileAttributeView(absPath, classOf[BasicFileAttributeView]).readAttributes()
    attrs.size == 0
  }

  /**
   * tests if source string starts with the configured remote target-dir
   *
   * @param source String
   * @return
   */
  def inRemoteRoot(source: String): Boolean = source.startsWith(s"$remoteDirName/")

  /**
   * tests if source string starts with the configured local target-dir
   *
   * @param source String
   * @return
   */
  def inLocalRoot(source: String): Boolean = source.startsWith(s"$localDirName/")

}
