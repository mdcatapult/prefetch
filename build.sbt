import Release._
import sbtrelease.ReleasePlugin.autoImport.ReleaseTransformations._

lazy val configVersion = "1.3.2"
lazy val akkaVersion = "2.6.4"
lazy val catsVersion = "2.1.0"
lazy val awsScalaVersion = "0.8.4"
lazy val betterFilesVersion = "3.8.0"
lazy val doclibCommonVersion = "1.0.2-SNAPSHOT"
lazy val prometheusClientVersion = "0.9.0"

val meta = """META.INF/(blueprint|cxf).*""".r

lazy val IntegrationTest = config("it") extend Test
concurrentRestrictions in Global += Tags.limit(Tags.Test, max = 1)

lazy val root = (project in file("."))
  .configs(IntegrationTest)
  .settings(
    Defaults.itSettings,
    name := "consumer-prefetch",
    scalaVersion := "2.13.3",
    scalacOptions ++= Seq(
      "-encoding", "utf-8",
      "-unchecked",
      "-deprecation",
      "-explaintypes",
      "-feature",
      "-Xlint",
      "-Xfatal-warnings",
    ),
    useCoursier := false,
    resolvers ++= Seq(
      "MDC Nexus Releases" at "https://nexus.mdcatapult.io/repository/maven-releases/",
      "MDC Nexus Snapshots" at "https://nexus.mdcatapult.io/repository/maven-snapshots/"),
    updateOptions := updateOptions.value.withLatestSnapshots(latestSnapshots = false),
    credentials += {
      sys.env.get("NEXUS_PASSWORD") match {
        case Some(p) =>
          Credentials("Sonatype Nexus Repository Manager", "nexus.mdcatapult.io", "gitlab", p)
        case None =>
          Credentials(Path.userHome / ".sbt" / ".credentials")
      }
    },
    libraryDependencies ++= Seq(
      "org.scalactic" %% "scalactic" % "3.1.1",
      "org.scalatest" %% "scalatest" % "3.1.1" % "it,test",
      "org.scalamock" %% "scalamock" % "4.4.0" % "it,test",
      "com.typesafe.akka" %% "akka-testkit" % akkaVersion % "it,test",
      "com.typesafe.akka" %% "akka-slf4j" % akkaVersion,
      "com.typesafe.akka" %% "akka-http" % "10.1.11",
      "com.lightbend.akka" %% "akka-stream-alpakka-ftp" % "2.0.0",
      "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2",
      "com.typesafe" % "config" % configVersion,
      "ch.qos.logback" % "logback-classic" % "1.2.3",
      "org.typelevel" %% "cats-macros" % catsVersion,
      "org.typelevel" %% "cats-kernel" % catsVersion,
      "org.typelevel" %% "cats-core" % catsVersion,
      "io.mdcatapult.doclib" %% "common" % doclibCommonVersion,
      "io.mdcatapult.klein" %% "util" % "1.0.0",
      "com.github.seratch" %% "awscala" % awsScalaVersion,
      "com.github.pathikrit" %% "better-files" % betterFilesVersion,
      "com.github.jai-imageio" % "jai-imageio-jpeg2000" % "1.3.0",
      "org.xerial" % "sqlite-jdbc" % "3.30.1",
      "io.prometheus" % "simpleclient" % prometheusClientVersion,
      "io.prometheus" % "simpleclient_hotspot" % prometheusClientVersion,
      "io.prometheus" % "simpleclient_httpserver" % prometheusClientVersion
    ).map(
      _.exclude(org = "com.google.protobuf", name = "protobuf-java")
        .exclude(org = "io.netty", name = "netty-all")
      // IMPORTANT! netty must be excluded to avoid conflict with "sbt assembly", but is needed at runtime for SSL.
      //            To get around this CI uses wget to download this jar and it gets copied into docker.
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
