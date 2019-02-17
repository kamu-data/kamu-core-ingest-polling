name := "ingest-polling"
version := "0.0.1"

// For testing with Spark
test in assembly := {}
fork in Test := true
parallelExecution in Test := false
javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:+CMSClassUnloadingEnabled")


libraryDependencies ++= Seq(
  // Config
  "com.github.pureconfig" %% "pureconfig" % "0.10.1",

  // HTTP Utils
  "com.softwaremill.sttp" %% "core" % "1.5.10",

  // Serialization
  "org.json4s" %% "json4s-jackson" % "3.5.3",

  // Spark
  "org.apache.spark" %% "spark-core" % Versions.spark % "provided",
  "org.apache.spark" %% "spark-sql" % Versions.spark % "provided",

  // GeoSpark
  "org.datasyslab" % "geospark" % "1.1.3",
  "org.datasyslab" % "geospark-sql_2.3" % "1.1.3",

  // Testing
  "org.scalatest" %% "scalatest" % "3.0.5" % "test",
  "com.holdenkarau" %% "spark-testing-base" % s"${Versions.spark}_0.11.0" % "test",
  "org.apache.spark" %% "spark-hive" % Versions.spark % "test"
)


dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.7.1"
