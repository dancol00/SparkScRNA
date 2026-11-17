name := "ScRNA Pipeline"
version := "1.0"
// Match the target server exactly: Spark 3.5.9 is a Scala 2.12 build.
scalaVersion := "2.12.18"

Compile / run / javaOptions ++= Seq("-Xmx20G")//, "-XX:StartFlightRecording,filename=recording_20g_newpca.jfr")

// Spark is supplied by spark-submit at runtime, so it is "provided":
// excluded from the assembly jar, but still needed for `sbt run` (see below).
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.5.9" % "provided",
  "org.apache.spark" %% "spark-mllib" % "3.5.9" % "provided"
)
// libraryDependencies +="org.scala-lang.modules" %% "scala-parallel-collections" % "1.2.0"
// libraryDependencies ++= Seq(
//   "org.scalanlp" %% "breeze" % "2.1.0",
//   "org.scalanlp" %% "breeze-viz" % "2.1.0"
// )
// libraryDependencies += "org.scala-lang" %% "toolkit" % "0.7.0"
// libraryDependencies += "com.nrinaudo" %% "kantan.csv" % "0.8.0"

import sbtassembly.AssemblyPlugin
import sbtassembly.MergeStrategy

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case _                              => MergeStrategy.first
}

// `sbt run` uses the Runtime classpath, which drops "provided" deps; run on the
// Compile classpath instead so Spark is visible in local `sbt run` as well.
Compile / run := Defaults.runTask(Compile / fullClasspath, Compile / run / mainClass, Compile / run / runner).evaluated

// spark-submit supplies Spark, Hadoop and the Scala library at runtime;
// the fat jar only carries first-party code plus libs Spark does not provide
// (breeze, kantan, ...). "provided" deps are automatically excluded.
assembly / assemblyJarName := "scrna-pipeline.jar"
assembly / assemblyOption := (assembly / assemblyOption).value.withIncludeScala(false)
