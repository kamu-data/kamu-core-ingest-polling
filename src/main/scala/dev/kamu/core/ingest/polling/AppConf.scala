package dev.kamu.core.ingest.polling

import java.io.InputStream

import dev.kamu.core.manifests.{
  Manifest,
  DataSourcePolling,
  RepositoryVolumeMap
}

case class AppConf(
  repository: RepositoryVolumeMap,
  sources: Vector[DataSourcePolling] = Vector.empty
)

object AppConf {
  import dev.kamu.core.manifests.parsing.pureconfig.yaml
  import yaml.defaults._
  import pureconfig.generic.auto._

  val configFileName = "poll-config.yaml"
  val pollCacheFileName = "poll-cache.json"

  val repositoryConfigFile = "repositoryVolumeMap.yaml"
  val dataSourceConfigFile = "dataSourcePolling.yaml"

  private def getConfigFromResources(configFileName: String): InputStream = {
    val configStream =
      getClass.getClassLoader.getResourceAsStream(configFileName)

    if (configStream == null)
      throw new RuntimeException(
        s"Unable to locate $configFileName on classpath"
      )

    configStream
  }

  def load(): AppConf = {
    val source = yaml
      .load[Manifest[DataSourcePolling]](
        getConfigFromResources(dataSourceConfigFile)
      )
      .content

    val repository = yaml
      .load[Manifest[RepositoryVolumeMap]](
        getConfigFromResources(repositoryConfigFile)
      )
      .content

    val appConfig = AppConf(
      repository = repository,
      sources = Vector(source)
    )

    appConfig
  }
}
