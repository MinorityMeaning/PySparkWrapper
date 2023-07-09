ThisBuild / version := "0.9.0"

ThisBuild / scalaVersion := "2.12.15"

lazy val root = (project in file("."))
  .settings(
    name := "PySparkWrapper"
  )

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.2.1" % Provided
)