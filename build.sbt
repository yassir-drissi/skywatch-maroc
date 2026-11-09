// Build sbt du projet SkyWatch Maroc (partie Scala / Spark).
// Le code Python reste dans ingestion/ ; le code Scala est dans streaming/.

ThisBuild / scalaVersion := "2.12.20" // Spark 3.5 et Databricks utilisent Scala 2.12
ThisBuild / organization := "ma.skywatch"
ThisBuild / version      := "0.1.0"

// Même version que l'image Docker spark:3.5.7-scala2.12-java17-ubuntu
val sparkVersion = "3.5.7"

lazy val streaming = (project in file("streaming"))
  .settings(
    name := "skywatch-streaming",
    scalacOptions ++= Seq("-encoding", "utf8", "-deprecation", "-feature", "-unchecked"),
    libraryDependencies ++= Seq(
      // « Provided » : Spark est déjà dans l'image Docker, on ne le met pas dans notre jar
      "org.apache.spark" %% "spark-sql"            % sparkVersion % Provided,
      "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion % Provided,
      "org.scalatest"    %% "scalatest"            % "3.2.19"     % Test
    )
  )