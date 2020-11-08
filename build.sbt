organization := "ch.hepia"
val neotypesV = "0.4.0"

name := "parser"

version := "0.0.1"

scalaVersion := "2.12.10"

mainClass := Some("ch.hepia.Main")

libraryDependencies ++= Seq(
  "io.spray" %%  "spray-json" % "1.3.5",
  "com.dimafeng" %% "neotypes" % neotypesV
)
