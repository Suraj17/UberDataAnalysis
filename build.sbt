ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.11.12"

name := "UberDataAnalysis"

//lazy val root = (project in file("."))
//  .settings(
//    name := "UberDataAnalysis"
//  )

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.8"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
//libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.8" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.8"

// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming
//libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.4.8" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.4.8"

