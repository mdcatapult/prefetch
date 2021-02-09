package io.mdcatapult.doclib.util

import com.typesafe.config.Config

object Common {
  private var config: Config = Config

  private var doclibRoot = ""
  private var archiveDirName = ""
  private var localDirName = ""
  private var remoteDirName = ""

  def init(conf: Config): Unit =  {
    config = conf
    doclibRoot = s"${config.getString("doclib.root").replaceFirst("""/+$""", "")}/"
    archiveDirName = config.getString("doclib.archive.target-dir")
    localDirName = config.getString("doclib.local.target-dir")
    remoteDirName = config.getString("doclib.remote.target-dir")
  }

  def getDoclibRoot: String = {
    doclibRoot
  }

  def getArchiveDirName: String = {
    archiveDirName
  }

  def getLocalDirName: String = {
    localDirName
  }

  def getRemoteDirName: String = {
    remoteDirName
  }

}
