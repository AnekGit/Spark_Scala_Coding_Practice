/*
name := "HelloSpark"
organization := "guru.learningjournal"
version := "0.1"
scalaVersion := "2.12.10"
//scalaVersion := "2.11.8"
autoScalaLibrary := false
val sparkVersion = "3.0.0-preview2"
//val sparkVersion = "2.3.1"

val sparkDependencies = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion ,
  "com.oracle" % "ojdbc7" % "12.1.0.2"
)

val testDependencies = Seq(
  "org.scalatest" %% "scalatest" % "3.0.8" % Test
)
val hiveDependencies = Seq(
  "org.apache.spark" %% "spark-hive_2.12" % "Compile"
)
libraryDependencies += "io.delta" %% "delta-core" % "0.7.0" % "provided"
libraryDependencies ++= sparkDependencies ++ testDependencies ++ hiveDependencies

// https://mvnrepository.com/artifact/org.apache.spark/spark-hive
//libraryDependencies += "org.apache.spark" %% "spark-hive" % "1.0.0" % "provided"
// https://mvnrepository.com/artifact/com.oracle/ojdbc7

*/

name := "HelloSpark"
organization := "guru.learningjournal"
version := "0.1"
scalaVersion := "2.12.10"
autoScalaLibrary := false
val sparkVersion = "3.0.0-preview2"


val sparkDependencies = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion ,
  "com.oracle.ojdbc" % "ojdbc8" % "19.3.0.0"
)

val testDependencies = Seq(
  "org.scalatest" %% "scalatest" % "3.0.8" % Test
)
val hiveDependencies = Seq(
  "org.apache.spark" %% "spark-hive" % "3.0.0-preview2"
)


libraryDependencies += "io.delta" %% "delta-core" % "0.7.0" % "provided"
libraryDependencies ++= sparkDependencies ++ testDependencies ++ hiveDependencies

// https://mvnrepository.com/artifact/org.apache.spark/spark-hive
//libraryDependencies += "org.apache.spark" %% "spark-hive" % "1.0.0" % "provided"
// https://mvnrepository.com/artifact/com.oracle/ojdbc7

