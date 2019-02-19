import java.net.URI
import java.nio.file.Path

import pureconfig.generic.ProductHint
import pureconfig.module.yaml.loadYamlOrThrow
import pureconfig.{CamelCase, ConfigFieldMapping}
import pureconfig.generic.auto._


object Source {
  val DEFAULT_READER_OPTIONS: Map[String, String] = Map(
    "mode" -> "FAILFAST"
  )
}


case class Source(
  id: String,
  url: URI,
  compression: Option[String] = None,
  subPath: Option[Path] = None,
  format: String,

  /** Options to pass into the [[org.apache.spark.sql.DataFrameReader]]
    *
    * Options in config will be merged with [[Source.DEFAULT_READER_OPTIONS]].
    */
  readerOptions: Map[String, String] = Map.empty,

  /** A DDL-formatted schema that can be used to cast values into
    * more appropriate data types.
    */
  schema: Vector[String] = Vector.empty
)


case class AppConfig(
  /** Directory to store downloaded data in before processing */
  downloadDir: URI,

  /** Directory to store cache information not to re-download
    * data if it didn't change
    */
  checkpointDir: URI,

  /** Root data set directory for ingested raw data */
  dataDir: URI,

  /** List of sources to poll */
  sources: Vector[Source] = Vector.empty
)

object AppConfig {
  val configFileName = "poll-config.yaml"

  def load(): AppConfig = {
    implicit def hint[T]: ProductHint[T] =
      ProductHint[T](ConfigFieldMapping(CamelCase, CamelCase))

    val configStream = getClass.getClassLoader.getResourceAsStream(configFileName)

    if (configStream == null)
      throw new RuntimeException(
        s"Unable to locate $configFileName on classpath")

    val configString = scala.io.Source.fromInputStream(configStream).mkString

    val config = loadYamlOrThrow[AppConfig](configString)

    withDefaults(
      fixRelativeURLs(
        config))
  }

  def withDefaults(config: AppConfig): AppConfig = {
    config.copy(
      sources = config.sources.map(source =>
        source.copy(
          readerOptions = Source.DEFAULT_READER_OPTIONS ++ source.readerOptions)))
  }

  def fixRelativeURLs(config: AppConfig): AppConfig = {
    config.copy(
      downloadDir = fixRelativeURL(config.downloadDir),
      checkpointDir = fixRelativeURL(config.checkpointDir),
      dataDir = fixRelativeURL(config.dataDir))
  }

  def fixRelativeURL(url: URI): URI = {
    URI.create(url.toString.replaceFirst(
      "^file:\\.",
      s"file:${System.getProperty("user.dir")}/."
    ))
  }
}
