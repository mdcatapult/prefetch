import java.io.PrintWriter

import ReleaseTransformations._
import sbtrelease.{Vcs, Version}
import sbtassembly.AssemblyPlugin.defaultUniversalScript
import sbtrelease.ReleasePlugin.autoImport.ReleaseKeys.skipTests

lazy val configVersion = "1.3.2"
lazy val akkaVersion = "2.5.25"
lazy val catsVersion = "2.0.0"
lazy val opRabbitVersion = "2.1.0"
lazy val mongoVersion = "2.5.0"
lazy val awsScalaVersion = "0.8.1"
lazy val tikaVersion = "1.21"
lazy val betterFilesVersion = "3.8.0"
lazy val doclibCommonVersion = "0.0.22"

val meta = """META.INF/(blueprint|cxf).*""".r

lazy val IntegrationTest = config("it") extend(Test)

lazy val root = (project in file(".")).
  configs(IntegrationTest)
  .settings(
    Defaults.itSettings,
    name              := "consumer-prefetch",
    scalaVersion      := "2.12.8",
    scalacOptions     ++= Seq("-Ypartial-unification"),
    resolvers         ++= Seq("MDC Nexus Releases" at "http://nexus.mdcatapult.io/repository/maven-releases/", "MDC Nexus Snapshots" at "http://nexus.mdcatapult.io/repository/maven-snapshots/"),
    updateOptions     := updateOptions.value.withLatestSnapshots(false),
    credentials       += {
      val nexusPassword = sys.env.get("NEXUS_PASSWORD")
      if ( nexusPassword.nonEmpty ) {
        Credentials("Sonatype Nexus Repository Manager", "nexus.mdcatapult.io", "gitlab", nexusPassword.get)
      } else {
        Credentials(Path.userHome / ".sbt" / ".credentials")
      }
    },
    libraryDependencies ++= Seq(
      "org.scalactic" %% "scalactic"                  % "3.0.5",
      "org.scalatest" %% "scalatest"                  % "3.0.5" % "it,test",
      "org.scalamock" %% "scalamock"                  % "4.3.0" % "it,test",
      "com.typesafe.akka" %% "akka-testkit"           % akkaVersion % "it,test",
      "com.typesafe.akka" %% "akka-actor"             % akkaVersion,
      "com.typesafe.akka" %% "akka-slf4j"             % akkaVersion,
      "com.lightbend.akka" %% "akka-stream-alpakka-ftp" % "1.1.1",
      "com.typesafe.play" %% "play-ahc-ws-standalone" % "2.0.3",
      "com.typesafe.scala-logging" %% "scala-logging" % "3.9.0",
      "com.typesafe" % "config"                       % configVersion,
      "ch.qos.logback" % "logback-classic"            % "1.2.3",
      "org.typelevel" %% "cats-macros"                % catsVersion,
      "org.typelevel" %% "cats-kernel"                % catsVersion,
      "org.typelevel" %% "cats-core"                  % catsVersion,
      "io.mdcatapult.doclib" %% "common"              % doclibCommonVersion,
      "com.github.seratch" %% "awscala"               % awsScalaVersion,
      "com.github.pathikrit"  %% "better-files"       % betterFilesVersion,
    ),
  )
  .settings(
    assemblyJarName := "consumer-prefetch.jar",
    test in assembly := {},
    assemblyMergeStrategy in assembly := {
      case PathList("com", "sun", xs @ _*) => MergeStrategy.first
      case PathList("javax", "servlet", xs @ _*) => MergeStrategy.first
      case PathList("javax", "activation", xs @ _*) => MergeStrategy.first
      case PathList(ps @ _*) if ps.last endsWith ".html" => MergeStrategy.first
      case PathList(xs @ _*) if xs.last == "module-info.class" => MergeStrategy.first
      case PathList("org", "apache", "commons", xs @ _*) => MergeStrategy.first
      case PathList("com", "ctc", "wstx", xs @ _*) => MergeStrategy.first
      case PathList(xs @ _*) if xs.last == "public-suffix-list.txt" => MergeStrategy.first
      case PathList(xs @ _*) if xs.last == ".gitkeep" => MergeStrategy.discard
      case "META-INF/jpms.args" => MergeStrategy.discard
      case n if n.startsWith("application.conf") => MergeStrategy.concat
      case n if n.endsWith(".conf") => MergeStrategy.concat
      case meta(_) => MergeStrategy.first
      case x =>
        val oldStrategy = (assemblyMergeStrategy in assembly).value
        oldStrategy(x)
    }
  )
  .settings(
    releaseProcess := Seq[ReleaseStep](
      checkSnapshotDependencies,
      inquireVersions,
      runClean,
      runTest,
      setReleaseVersion,
      { st: State ⇒
        val extracted: Extracted = Project.extract( st )
        val vcs: Vcs = extracted.get(releaseVcs)
          .getOrElse(sys.error("Aborting release. Working directory is not a repository of a recognized VCS."))
        st.put(AttributeKey[String]("hash"), vcs.currentHash.slice(0, 8))
      },
      { st: State ⇒
        // write version.conf
        st.get(ReleaseKeys.versions) match {
          case Some(v) ⇒ writeVersionFile(v._1, st.get(AttributeKey[String]("hash")))
          case None ⇒ sys.error("Aborting release. no version number present.")
        }
        st
      },
//      commitReleaseVersion,
//      tagRelease,
      { st: State =>
        val extracted = Project.extract(st)
        val ref = extracted.get(thisProjectRef)
        extracted.runAggregated(assembly in Global in ref, st)
      },

//      setNextVersion,
      { st: State ⇒
        // write version.conf
        st.get(ReleaseKeys.versions) match {
          case Some(v) ⇒ writeVersionFile(v._2)
          case None ⇒ sys.error("Aborting release. no version number present.")
        }
        st
      },
//      commitNextVersion,
//      pushChanges
    )
  )

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