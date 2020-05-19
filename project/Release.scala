import java.io.PrintWriter

import sbt.Keys._
import sbt._
import sbtassembly.AssemblyKeys._
import sbtrelease.ReleasePlugin.autoImport._
import sbtrelease._

import scala.sys.process.ProcessLogger

object Release {

  def getShortSha: State => State = { st: State =>
    val extracted: Extracted = Project.extract(st)
    val vcs: Vcs = extracted.get(releaseVcs)
      .getOrElse(sys.error("Aborting release. Working directory is not a repository of a recognized VCS."))
    st.put(AttributeKey[String]("hash"), vcs.currentHash.slice(0, 8))
  }



  def runAssembly: ReleaseStep = ReleaseStep(action = (st: State) => {
    val extracted = Project.extract(st)
    val ref = extracted.get(thisProjectRef)
    extracted.runAggregated(assembly in Global in ref, st)
  })

  def commitAllRelease: ReleaseStep = ReleaseStep(action = (st: State) => commitAll(st, releaseCommitMessage))

  def commitAllNext: ReleaseStep = ReleaseStep(action = (st: State) => commitAll(st, releaseNextCommitMessage))

  def commitAll: (State, TaskKey[String]) => State = { (st: State, commitMessage: TaskKey[String]) =>
    val log = toTempProcessLogger(st)
    val extract = Project.extract(st)
    val vcs = getVcs(st)
    val sign = extract.get(releaseVcsSign)
    val signOff = extract.get(releaseVcsSignOff)
    vcs.add("./*") !! log
    val status = vcs.status.!!.trim
    val newState = if (status.nonEmpty) {
      val (state, msg) = extract.runTask(commitMessage, st)
      vcs.commit(msg, sign, signOff) ! log
      state
    } else {
      // nothing to commit. this happens if the version.sbt file hasn't changed.
      st
    }
    newState
  }

  def getVcs(st: State): Vcs = {
    val extracted: Extracted = Project.extract(st)
    extracted.get(releaseVcs)
      .getOrElse(sys.error("Aborting release. Working directory is not a repository of a recognized VCS."))
  }

  def toTempProcessLogger(st: State): ProcessLogger = new ProcessLogger {
    override def err(s: => String): Unit = st.log.info(s)

    override def out(s: => String): Unit = st.log.info(s)

    override def buffer[T](f: => T): T = st.log.buffer(f)
  }

  def writeReleaseVersionFile: ReleaseStep = ReleaseStep(action= (st: State) => {
    // write version.conf
    st.get(ReleaseKeys.versions) match {
      case Some(v) => writeVersionFile(v._1, st.get(AttributeKey[String]("hash")))
      case None => sys.error("Aborting release. no version number present.")
    }
    st
  })

  def writeNextVersionFile: ReleaseStep = ReleaseStep(action= (st: State) => {
    // write version.conf
    st.get(ReleaseKeys.versions) match {
      case Some(v) => writeVersionFile(v._2, st.get(AttributeKey[String]("hash")))
      case None => sys.error("Aborting release. no version number present.")
    }
    st
  })

  def writeVersionFile(version: String, hash: Option[String] = None): Unit = {
    val ver: Version = Version(version).get
    val writer = new PrintWriter(new File("src/main/resources/version.conf"))
    writer.write(
      s"""version {
         |  number = "${ver.string}",
         |  major = ${ver.major},
         |  minor =  ${ver.subversions.head},
         |  patch = ${ver.subversions(1)},
         |  hash =  "${hash.getOrElse(ver.qualifier.get.replaceAll("^-", ""))}"
         |  hash =  $${?VERSION_HASH}
         |}
         |""".stripMargin)
    writer.close()
  }
}
