name := "sparkql"

version := "1.0"

scalaVersion := "2.10.2"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.5.2"

libraryDependencies += "org.apache.spark" %% "spark-graphx" % "1.5.2"

scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature")
