package dev.kamu.core.ingest.polling

import java.net.URI

import dev.kamu.core.manifests.DataSourcePolling
import org.apache.hadoop.fs.Path
import pureconfig.generic.ProductHint
import pureconfig.module.yaml.loadYamlOrThrow
import pureconfig.{CamelCase, ConfigFieldMapping, ConfigReader}

case class AppConf(
  /** Directory to store downloaded data in before processing */
  downloadDir: Path,
  /** Directory to store cache information in */
  checkpointDir: Path,
  /** Root data set directory for ingested raw data */
  dataDir: Path,
  /** List of sources to poll */
  sources: Vector[DataSourcePolling] = Vector.empty
) {
  def withDefaults(): AppConf = {
    copy(sources = sources.map(source => source.withDefaults()))
  }
}

object AppConf {
  import pureconfig.generic.auto._

  val configFileName = "poll-config.yaml"
  val pollCacheFileName = "poll-cache.json"

  implicit val pathReader = ConfigReader[String]
    .map(s => new Path(URI.create(s)))

  implicit def hint[T]: ProductHint[T] =
    ProductHint[T](ConfigFieldMapping(CamelCase, CamelCase),
                   useDefaultArgs = true,
                   allowUnknownKeys = false)

  def load(): AppConf = {
    val configStream =
      getClass.getClassLoader.getResourceAsStream(configFileName)
    if (configStream == null)
      throw new RuntimeException(
        s"Unable to locate $configFileName on classpath")

    val configString = scala.io.Source.fromInputStream(configStream).mkString
    val config = loadYamlOrThrow[AppConf](configString)

    config.withDefaults()
  }

}
