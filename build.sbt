name := "sparkconn"

version := "0.1"

scalaVersion := "2.11.8"

val scalaTestVersion = "3.0.4"

val sparkVersion = "2.3.1"

val json4sVersion = "3.2.11"

libraryDependencies += "org.scalatest" %% "scalatest" % scalaTestVersion % "test"

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion  % "provided"

libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion  % "provided"

libraryDependencies += "org.apache.spark" %% "spark-catalyst" % sparkVersion % "provided"

libraryDependencies += "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided"

libraryDependencies += "org.json4s" %% "json4s-jackson" % json4sVersion % "provided"


