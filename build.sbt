lazy val kamuCoreManifests = RootProject(file("../kamu-core-manifests"))

lazy val kamuCoreIngestPolling = (project in file("."))
  .aggregate(kamuCoreManifests)
  .dependsOn(kamuCoreManifests)
  .settings(
    scalaVersion := "2.11.12",
    organization := "dev.kamu",
    organizationName := "kamu",
    name := "kamu-core-ingest-polling",
    version := "0.1.0-SNAPSHOT",
    libraryDependencies ++= Seq(
      // Internal
      "dev.kamu" %% "kamu-core-manifests" % "0.1.0-SNAPSHOT",
      // Config
      "com.github.pureconfig" %% "pureconfig" % "0.10.2",
      "com.github.pureconfig" %% "pureconfig-yaml" % "0.10.2",
      // HTTP Utils
      "org.scalaj" %% "scalaj-http" % "2.4.1",
      "commons-net" % "commons-net" % "3.6",
      // Spark
      "org.apache.spark" %% "spark-core" % Versions.spark % "provided",
      "org.apache.spark" %% "spark-sql" % Versions.spark % "provided",
      // GeoSpark
      "org.datasyslab" % "geospark" % Versions.geoSpark % "provided",
      "org.datasyslab" % "geospark-sql_2.3" % Versions.geoSpark % "provided",
      // Testing
      "org.scalatest" %% "scalatest" % "3.0.5" % "test",
      "com.holdenkarau" %% "spark-testing-base" % s"${Versions.spark}_0.11.0" % "test",
      "org.apache.spark" %% "spark-hive" % Versions.spark % "test"
    ),
    dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.7.1",
    // For testing with Spark
    test in assembly := {},
    fork in Test := true,
    parallelExecution in Test := false,
    javaOptions ++= Seq(
      "-Xms512M",
      "-Xmx2048M",
      "-XX:+CMSClassUnloadingEnabled"
    )
  )
