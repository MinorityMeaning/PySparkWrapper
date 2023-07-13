ThisBuild / version := "0.1.0"

ThisBuild / scalaVersion := "2.13.10"

organization := "ru.mardaunt"

licenses := Seq("APL2" -> url("https://www.apache.org/licenses/LICENSE-2.0.txt"))

description := "The library helps to link Scala Spark and PaySpark app"

import xerial.sbt.Sonatype._
sonatypeProjectHosting := Some(GitHubHosting("Mardaunt", "PySparkWrapper", "tepeshman@gmail.com"))

publishTo := sonatypePublishToBundle.value

sonatypeCredentialHost := "s01.oss.sonatype.org"
sonatypeRepository := "https://s01.oss.sonatype.org/service/local"

lazy val root = (project in file("."))
  .settings(
    name := "PySparkWrapper"
  )

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.2.1" % Provided
)
