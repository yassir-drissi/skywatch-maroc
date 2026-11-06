// Build sbt du projet SkyWatch Maroc (partie Scala / Spark).
// Le code Python reste dans ingestion/ ; le code Scala est dans streaming/.

ThisBuild / scalaVersion := "2.12.20" // Spark 3.5 et Databricks utilisent Scala 2.12
ThisBuild / organization := "ma.skywatch"
ThisBuild / version      := "0.1.0"

lazy val streaming = (project in file("streaming"))
  .settings(
    name := "skywatch-streaming",
    scalacOptions ++= Seq("-encoding", "utf8", "-deprecation", "-feature", "-unchecked"),
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % "3.2.19" % Test
    )
  )