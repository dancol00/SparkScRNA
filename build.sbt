name := "ScRNA Pipeline"
version := "1.0"
scalaVersion := "2.13.18"

Compile / run / javaOptions ++= Seq("-Xmx20G")//, "-XX:StartFlightRecording,filename=recording_20g_newpca.jfr")

libraryDependencies += "org.apache.spark" %% "spark-sql" % "4.1.2"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "4.1.2"
libraryDependencies +="org.scala-lang.modules" %% "scala-parallel-collections" % "1.2.0"
libraryDependencies ++= Seq(
  "org.scalanlp" %% "breeze" % "2.1.0",
  "org.scalanlp" %% "breeze-viz" % "2.1.0"
)
libraryDependencies += "org.scala-lang" %% "toolkit" % "0.7.0"
libraryDependencies += "com.nrinaudo" %% "kantan.csv" % "0.8.0"

import sbtassembly.AssemblyPlugin
import sbtassembly.MergeStrategy

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case _                              => MergeStrategy.first
}
