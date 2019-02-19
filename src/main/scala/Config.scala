import java.net.URI

import org.apache.hadoop.fs.Path
import pureconfig.generic.ProductHint
import pureconfig.module.yaml.loadYamlOrThrow
import pureconfig.{CamelCase, ConfigFieldMapping, ConfigReader}
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
  downloadDir: Path,

  /** Directory to store cache information not to re-download
    * data if it didn't change
    */
  checkpointDir: Path,

  /** Root data set directory for ingested raw data */
  dataDir: Path,

  /** List of sources to poll */
  sources: Vector[Source] = Vector.empty
)


object AppConfig {
  val configFileName = "poll-config.yaml"
  val pollCacheFileName = "poll-cache.json"

  // Reader for hadoop fs paths
  implicit val pathReader = ConfigReader[String]
    .map(s => new Path(URI.create(s)))

  // Hint to use camel case for config field names
  implicit def hint[T]: ProductHint[T] =
    ProductHint[T](ConfigFieldMapping(CamelCase, CamelCase))

  def load(): AppConfig = {
    val configStream = getClass.getClassLoader.getResourceAsStream(configFileName)
    if (configStream == null)
      throw new RuntimeException(
        s"Unable to locate $configFileName on classpath")

    val configString = scala.io.Source.fromInputStream(configStream).mkString
    val config = loadYamlOrThrow[AppConfig](configString)

    withDefaults(config)
  }

  def withDefaults(config: AppConfig): AppConfig = {
    config.copy(
      sources = config.sources.map(source =>
        source.copy(
          readerOptions = Source.DEFAULT_READER_OPTIONS ++ source.readerOptions)))
  }
}
