package dev.kamu.core.ingest.polling

import java.net.URI

import dev.kamu.core.manifests.{DataSourcePolling, RepositoryVolumeMap}
import org.apache.hadoop.fs.Path
import pureconfig.generic.ProductHint
import pureconfig.module.yaml.loadYamlOrThrow
import pureconfig.{CamelCase, ConfigFieldMapping, ConfigReader, Derivation}

import scala.reflect.ClassTag

case class AppConf(
  repository: RepositoryVolumeMap,
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

  val repositoryConfigFile = "repositoryVolumeMap.yaml"
  val dataSourceConfigFile = "dataSourcePolling.yaml"

  implicit val pathReader = ConfigReader[String]
    .map(s => new Path(URI.create(s)))

  implicit def hint[T]: ProductHint[T] =
    ProductHint[T](
      ConfigFieldMapping(CamelCase, CamelCase),
      useDefaultArgs = true,
      allowUnknownKeys = false
    )

  def load(): AppConf = {
    val repository = loadConfig[RepositoryVolumeMap](repositoryConfigFile)
    val source = loadConfig[DataSourcePolling](dataSourceConfigFile)

    val appConfig = AppConf(
      repository = repository,
      sources = Vector(source)
    )

    appConfig.withDefaults()
  }

  def loadConfig[T: ClassTag](
    configFileName: String
  )(implicit reader: Derivation[ConfigReader[T]]): T = {
    val configStream =
      getClass.getClassLoader.getResourceAsStream(configFileName)

    if (configStream == null)
      throw new RuntimeException(
        s"Unable to locate $configFileName on classpath"
      )

    val configString = scala.io.Source.fromInputStream(configStream).mkString
    loadYamlOrThrow[T](configString)
  }

}
