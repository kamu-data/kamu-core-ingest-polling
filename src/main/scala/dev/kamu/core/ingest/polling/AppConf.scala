package dev.kamu.core.ingest.polling

import java.io.InputStream

import dev.kamu.core.manifests.{
  Manifest,
  DataSourcePolling,
  RepositoryVolumeMap
}

case class AppConf(
  repository: RepositoryVolumeMap,
  sources: List[DataSourcePolling]
)

object AppConf {
  import dev.kamu.core.manifests.parsing.pureconfig.yaml
  import yaml.defaults._
  import pureconfig.generic.auto._

  val configFileName = "poll-config.yaml"
  val pollCacheFileName = "poll-cache.json"

  val repositoryConfigFile = "repositoryVolumeMap.yaml"

  def load(): AppConf = {
    val sources = findSources()

    val repository = yaml
      .load[Manifest[RepositoryVolumeMap]](
        getConfigFromResources(repositoryConfigFile)
      )
      .content

    val appConfig = AppConf(
      repository = repository,
      sources = sources
    )

    appConfig
  }

  // TODO: This sucks, but searching resources via pattern in Java is a pain
  private def findSources(
    index: Int = 0,
    tail: List[DataSourcePolling] = List.empty
  ): List[DataSourcePolling] = {
    val stream = getClass.getClassLoader.getResourceAsStream(
      s"dataSourcePolling_$index.yaml"
    )

    if (stream == null) {
      tail.reverse
    } else {
      val source = yaml.load[Manifest[DataSourcePolling]](stream).content
      findSources(index + 1, source :: tail)
    }
  }

  private def getConfigFromResources(configFileName: String): InputStream = {

    val configStream =
      getClass.getClassLoader.getResourceAsStream(configFileName)

    if (configStream == null)
      throw new RuntimeException(
        s"Unable to locate $configFileName on classpath"
      )

    configStream
  }
}
