name := "kamu-ingest-polling"
version := "0.0.1"
scalaVersion := "2.11.12"

val sparkVersion = "2.4.0"

libraryDependencies ++= Seq(
  // Config
  "com.github.pureconfig" %% "pureconfig" % "0.10.1",

  // HTTP Utils
  "org.scalaj" %% "scalaj-http" % "2.4.1",
  "commons-io" % "commons-io" % "2.6",

  // Serialization
  "org.json4s" %% "json4s-jackson" % "3.5.3",

  // Spark
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
  //"org.scala-lang.modules" %% "scala-xml" % "1.1.1"
)
