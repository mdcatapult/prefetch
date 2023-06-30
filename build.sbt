import Release._
import sbtrelease.ReleasePlugin.autoImport.ReleaseTransformations._

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
      "-Xlint"
//      "-Xfatal-warnings", This was removed due to scalamock complaining about deprecated traits
      //      in the mongo libs that we stub in the tests.
    ),
    useCoursier := false,
    resolvers ++= Seq(
      "MDC Nexus Releases" at "https://nexus.wopr.inf.mdc/repository/maven-releases/",
      "MDC Nexus Snapshots" at "https://nexus.wopr.inf.mdc/repository/maven-snapshots/"),
    updateOptions := updateOptions.value.withLatestSnapshots(latestSnapshots = false),
    credentials += {
      sys.env.get("NEXUS_PASSWORD") match {
        case Some(p) =>
          Credentials("Sonatype Nexus Repository Manager", "nexus.wopr.inf.mdc", "gitlab", p)
        case None =>
          Credentials(Path.userHome / ".sbt" / ".credentials")
      }
    },
    libraryDependencies ++= {
      val doclibCommonVersion = "3.1.2-SNAPSHOT"

      val configVersion = "1.4.2"
      val akkaVersion = "2.8.1"
      val catsVersion = "2.9.0"
      val scalacticVersion = "3.2.15"
      val scalaTestVersion = "3.2.15"
      val scalaMockVersion = "5.2.0"
      val scalaLoggingVersion = "3.9.5"
      val logbackClassicVersion = "1.4.7"
      val betterFilesVersion = "3.9.2"
      val jaiImageJPEG2000Version = "1.4.0"
      val akkaHttpVersion = "10.5.0"
      val akkaStreamAlpakkaFTPVersion = "5.0.0"

      Seq(
      "org.scalactic" %% "scalactic" % scalacticVersion,
      "org.scalatest" %% "scalatest" % scalaTestVersion % "it,test",
      "org.scalamock" %% "scalamock" % scalaMockVersion % "it,test",
      "com.typesafe.akka" %% "akka-testkit" % akkaVersion % "it,test",
      "com.typesafe.akka" %% "akka-slf4j" % akkaVersion,
      "com.typesafe.akka" %% "akka-http" % akkaHttpVersion,
      "com.lightbend.akka" %% "akka-stream-alpakka-ftp" % akkaStreamAlpakkaFTPVersion,
      "com.typesafe.scala-logging" %% "scala-logging" % scalaLoggingVersion,
      "com.typesafe" % "config" % configVersion,
      "ch.qos.logback" % "logback-classic" % logbackClassicVersion,
      "org.typelevel" %% "cats-kernel" % catsVersion,
      "org.typelevel" %% "cats-core" % catsVersion,
      "io.mdcatapult.doclib" %% "common" % doclibCommonVersion,
      "com.github.pathikrit" %% "better-files" % betterFilesVersion,
      "com.github.jai-imageio" % "jai-imageio-jpeg2000" % jaiImageJPEG2000Version
//      "org.xerial" % "sqlite-jdbc" % "3.30.1"  - only required to suppress a tika warning. We are not parsing sqlite files
    )
    }.map(
      _.exclude(org = "com.google.protobuf", name = "protobuf-java")
        .exclude(org = "io.netty", name = "netty-all")
      // IMPORTANT! netty must be excluded to avoid conflict with "sbt assembly", but is needed at runtime for SSL.
      //            To get around this CI uses wget to download this jar and it gets copied into docker.
    ),
  )
  .settings(
    assemblyJarName := "consumer.jar",
    assembly / test := {},
    assembly / assemblyMergeStrategy := {
      case PathList("com", "sun", _*) => MergeStrategy.first
      case PathList("javax", "servlet", _*) => MergeStrategy.first
      case PathList("javax", "activation", _*) => MergeStrategy.first
      case PathList(ps@_*) if ps.last endsWith ".html" => MergeStrategy.first
      case PathList(xs@_*) if xs.last == "module-info.class" => MergeStrategy.first
      case PathList("org", "apache", "commons", _*) => MergeStrategy.first
      case PathList("com", "ctc", "wstx", _*) => MergeStrategy.first
      case PathList("scala", "collection", "compat", _*) => MergeStrategy.first
      case PathList("scala", "util", "control", "compat", _*) => MergeStrategy.first
      case PathList(xs@_*) if xs.last == "public-suffix-list.txt" => MergeStrategy.first
      case PathList(xs@_*) if xs.last == ".gitkeep" => MergeStrategy.discard
      case "META-INF/jpms.args" => MergeStrategy.discard
      case n if n.startsWith("application.conf") => MergeStrategy.first
      case n if n.startsWith("logback.xml") => MergeStrategy.first
      case n if n.endsWith(".conf") => MergeStrategy.concat
      case n if n.startsWith("scala-collection-compat.properties") => MergeStrategy.first
      case meta(_) => MergeStrategy.first
      case x =>
        val oldStrategy = (assembly / assemblyMergeStrategy).value
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
