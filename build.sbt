organization := "ch.hepia"
val neotypesV = "0.15.1"

name := "parser"

version := "0.0.1"

scalaVersion := "2.13.3"

mainClass := Some("ch.hepia.Main")

libraryDependencies ++= Seq(
  "io.spray" %%  "spray-json" % "1.3.5",
  "org.neo4j.driver" % "neo4j-java-driver" % "4.1.1",
  "com.dimafeng" %% "neotypes" % neotypesV,
  "org.scala-lang.modules" %% "scala-parallel-collections" % "0.2.0"
)
