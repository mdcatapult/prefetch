import Release._
import sbtrelease.ReleasePlugin.autoImport.ReleaseTransformations._

lazy val configVersion = "1.3.2"
lazy val akkaVersion = "2.5.26"
lazy val catsVersion = "2.1.0"
lazy val opRabbitVersion = "2.1.0"
lazy val mongoVersion = "2.5.0"
lazy val awsScalaVersion = "0.8.1"
lazy val tikaVersion = "1.21"
lazy val betterFilesVersion = "3.8.0"
lazy val doclibCommonVersion = "0.0.49"


val meta = """META.INF/(blueprint|cxf).*""".r

lazy val IntegrationTest = config("it") extend Test
concurrentRestrictions in Global += Tags.limit(Tags.Test, 1)

lazy val root = (project in file("."))
  .configs(IntegrationTest)
  .settings(
    Defaults.itSettings,
    //    Forking fixes a akka logging classloader test issue but sbt recommend setting ClassLoadLayeringStrategy instead to ScalaLibrary or Flat
    fork in Test := true,
    //    classLoaderLayeringStrategy in Test := ClassLoaderLayeringStrategy.Flat,
    name := "consumer-prefetch",
    scalaVersion := "2.12.10",
    scalacOptions ++= Seq(
      "-encoding", "utf-8",
      "-unchecked",
      "-deprecation",
      "-explaintypes",
      "-feature",
      "-Xlint",
      "-Ypartial-unification",
    ),
    resolvers ++= Seq(
      "MDC Nexus Releases" at "https://nexus.mdcatapult.io/repository/maven-releases/",
      "MDC Nexus Snapshots" at "https://nexus.mdcatapult.io/repository/maven-snapshots/"),
    updateOptions := updateOptions.value.withLatestSnapshots(false),
    credentials += {
      val nexusPassword = sys.env.get("NEXUS_PASSWORD")
      if (nexusPassword.nonEmpty) {
        Credentials("Sonatype Nexus Repository Manager", "nexus.mdcatapult.io", "gitlab", nexusPassword.get)
      } else {
        Credentials(Path.userHome / ".sbt" / ".credentials")
      }
    },
    libraryDependencies ++= Seq(
      "org.scalactic" %% "scalactic" % "3.0.5",
      "org.scalatest" %% "scalatest" % "3.0.5" % "it,test",
      "org.scalamock" %% "scalamock" % "4.3.0" % "it,test",
      "com.typesafe.akka" %% "akka-testkit" % akkaVersion % "it,test",
      "com.typesafe.akka" %% "akka-actor" % akkaVersion,
      "com.typesafe.akka" %% "akka-slf4j" % akkaVersion,
      "com.typesafe.akka" %% "akka-http" % "10.1.11",
      "com.lightbend.akka" %% "akka-stream-alpakka-ftp" % "1.1.1",
      "com.typesafe.play" %% "play-ahc-ws-standalone" % "2.0.3",
      "com.typesafe.scala-logging" %% "scala-logging" % "3.9.0",
      "com.typesafe" % "config" % configVersion,
      "ch.qos.logback" % "logback-classic" % "1.2.3",
      "org.typelevel" %% "cats-macros" % catsVersion,
      "org.typelevel" %% "cats-kernel" % catsVersion,
      "org.typelevel" %% "cats-core" % catsVersion,
      "io.mdcatapult.doclib" %% "common" % doclibCommonVersion,
      "com.github.seratch" %% "awscala" % awsScalaVersion,
      "com.github.pathikrit" %% "better-files" % betterFilesVersion
    ),
  )
  .settings(
    assemblyJarName := "consumer.jar",
    test in assembly := {},
    assemblyMergeStrategy in assembly := {
      case PathList("com", "sun", _*) => MergeStrategy.first
      case PathList("javax", "servlet", _*) => MergeStrategy.first
      case PathList("javax", "activation", _*) => MergeStrategy.first
      case PathList(ps@_*) if ps.last endsWith ".html" => MergeStrategy.first
      case PathList(xs@_*) if xs.last == "module-info.class" => MergeStrategy.first
      case PathList("org", "apache", "commons", _*) => MergeStrategy.first
      case PathList("com", "ctc", "wstx", _*) => MergeStrategy.first
      case PathList(xs@_*) if xs.last == "public-suffix-list.txt" => MergeStrategy.first
      case PathList(xs@_*) if xs.last == ".gitkeep" => MergeStrategy.discard
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
      getShortSha,
      writeReleaseVersionFile,
      commitAllRelease,
      tagRelease,
      runAssembly,
      setNextVersion,
      writeNextVersionFile,
      commitAllNext,
      pushChanges
    )
  )