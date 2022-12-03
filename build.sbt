ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.8",
  "org.apache.spark" %% "spark-sql" % "2.4.8",
  "org.postgresql" % "postgresql" % "42.5.0" % "runtime",
)




lazy val root = (project in file("."))
  .settings(
    name := "DE_Sprint_spark"
  )
