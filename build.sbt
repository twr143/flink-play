ThisBuild / resolvers ++= Seq(
  "Apache Development Snapshot Repository" at "https://repository.apache.org/content/repositories/snapshots/",
  Resolver.mavenLocal
)

name := "flink-play"

version := "0.1-SNAPSHOT"

organization := "org.iv"

ThisBuild / scalaVersion := "2.12.10"

val flinkVersion = "1.11.3"

val flinkDependencies = Seq(
  "org.apache.flink" %% "flink-clients" % flinkVersion,
  "org.apache.flink" %% "flink-scala" % flinkVersion,
  "org.apache.flink" %% "flink-streaming-scala" % flinkVersion,
  "org.apache.flink" %% "flink-connector-kafka" % flinkVersion,
  "org.apache.flink" %% "flink-connector-jdbc" % flinkVersion
)
val loggingDependencies = Seq(
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2",
  "ch.qos.logback" % "logback-classic" % "1.2.3" % Provided,
  "org.codehaus.janino" % "janino" % "3.1.0",
  "de.siegmar" % "logback-gelf" % "2.1.2",
)

lazy val root = (project in file(".")).
  settings(
    libraryDependencies ++= flinkDependencies ++ loggingDependencies ++ pgDeps,
    semanticdbEnabled := true, // enable SemanticDB
    semanticdbVersion := scalafixSemanticdb.revision, // use Scalafix compatible version
    scalacOptions += "-Ywarn-unused-import", // required by `RemoveUnused` rule
    scalacOptions += "-Yrangepos"
  )

assembly / mainClass := Some("org.iv.Job")

// make run command include the provided dependencies
Compile / run := Defaults.runTask(Compile / fullClasspath,
  Compile / run / mainClass,
  Compile / run / runner
).evaluated

// stays inside the sbt console when we press "ctrl-c" while a Flink programme executes with "run" or "runMain"
Compile / run / fork := true
Global / cancelable := true

// exclude Scala library from assembly
assembly / assemblyOption := (assembly / assemblyOption).value.copy(includeScala = false)
