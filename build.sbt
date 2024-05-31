import Release._
import sbtrelease.ReleasePlugin.autoImport.ReleaseTransformations._

lazy val scala_2_13 = "2.13.14"

val meta = """META.INF/(blueprint|cxf).*""".r

concurrentRestrictions in Global += Tags.limit(Tags.Test, max = 1)

val doclibCommonVersion = "5.0.1"

val configVersion = "1.4.3"
val pekkoVersion = "1.0.2"
val tikaVersion = "2.9.2"
val pekkoHttpVersion = "1.0.1"
val akkaVersion = "2.8.1"
val catsVersion = "2.10.0"
val scalacticVersion = "3.2.18"
val scalaTestVersion = "3.2.18"
val scalaMockVersion = "6.0.0"
val scalaLoggingVersion = "3.9.5"
val logbackClassicVersion = "1.5.6"
val betterFilesVersion = "3.9.2"
val jaiImageJPEG2000Version = "1.4.0"
val akkaHttpVersion = "10.5.0"
val akkaStreamAlpakkaFTPVersion = "5.0.0"
val apacheCommons = "4.4"

lazy val root = (project in file("."))
  .settings(
    name := "consumer-prefetch",
    scalaVersion := scala_2_13,
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
    resolvers         ++= Seq(
      "gitlab" at "https://gitlab.com/api/v4/projects/50550924/packages/maven",
      "Maven Public" at "https://repo1.maven.org/maven2"),
    updateOptions     := updateOptions.value.withLatestSnapshots(latestSnapshots = false),
    credentials       += {
      sys.env.get("CI_JOB_TOKEN") match {
        case Some(p) =>
          Credentials("GitLab Packages Registry", "gitlab.com", "gitlab-ci-token", p)
        case None =>
          Credentials(Path.userHome / ".sbt" / ".credentials")
      }
    },
    libraryDependencies ++= {
      Seq(
        "org.scalactic" %% "scalactic" % scalacticVersion,
        "org.scalatest" %% "scalatest" % scalaTestVersion % "test",
        "org.scalamock" %% "scalamock" % scalaMockVersion % "test",
        "org.apache.pekko" %% "pekko-testkit" % pekkoVersion % "test",
        "org.apache.pekko" %% "pekko-slf4j" % pekkoVersion,
        "org.apache.pekko" %% "pekko-http" % pekkoHttpVersion,
        "org.apache.pekko" %% "pekko-connectors-ftp" % pekkoVersion,
        "org.apache.tika" % "tika-core"                 % tikaVersion,
        "com.typesafe.scala-logging" %% "scala-logging" % scalaLoggingVersion,
        "com.typesafe" % "config" % configVersion,
        "ch.qos.logback" % "logback-classic" % logbackClassicVersion,
        "org.typelevel" %% "cats-kernel" % catsVersion,
        "org.typelevel" %% "cats-core" % catsVersion,
        "io.mdcatapult.doclib" %% "common" % doclibCommonVersion,
        "com.github.pathikrit" %% "better-files" % betterFilesVersion,
        "com.github.jai-imageio" % "jai-imageio-jpeg2000" % jaiImageJPEG2000Version,
        "org.apache.commons" % "commons-collections4" % apacheCommons
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

lazy val it = project
  .in(file("it"))  //it test located in a directory named "it"
  .settings(
      name := "consumer-prefetch-it",
    scalaVersion := "2.13.14",
    libraryDependencies ++= {
      Seq(
        "org.scalatest" %% "scalatest" % scalaTestVersion,
        "org.scalamock" %% "scalamock" % scalaMockVersion,
        "org.apache.pekko" %% "pekko-testkit" % pekkoVersion
      )
    }
  )
  .dependsOn(root)
