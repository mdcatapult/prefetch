package io.mdcatapult.doclib.util

import com.typesafe.config.Config

object FileConfig {

  case class SharedConfig(doclibRoot: String,
                          archiveDirName: String,
                          localDirName: String,
                          remoteDirName: String)

  def getSharedConfig(conf: Config): SharedConfig = {
    SharedConfig(
      doclibRoot = s"${conf.getString("doclib.root").replaceFirst("""/+$""", "")}/",
      archiveDirName = conf.getString("doclib.archive.target-dir"),
      localDirName = conf.getString("doclib.local.target-dir"),
      remoteDirName = conf.getString("doclib.remote.target-dir")
    )
  }
}
