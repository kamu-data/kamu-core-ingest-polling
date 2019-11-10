package dev.kamu.core.ingest.polling

import java.io.InputStream

import dev.kamu.core.manifests.{Dataset, Manifest, VolumeMap}

case class AppConf(
  volumeMap: VolumeMap,
  datasets: List[Dataset]
)

object AppConf {
  import dev.kamu.core.manifests.parsing.pureconfig.yaml
  import yaml.defaults._
  import pureconfig.generic.auto._

  val configFileName = "poll-config.yaml"
  val downloadCheckpointFileName = "download.json"
  val downloadDataFileName = "downloaded.bz2"
  val prepCheckpointFileName = "prepare.json"
  val prepDataFileName = "ready.bz2"
  val ingestCheckpointFileName = "ingest.json"

  val repositoryConfigFile = "repositoryVolumeMap.yaml"

  def load(): AppConf = {
    val datasets = findSources()

    val volumeMap = yaml
      .load[Manifest[VolumeMap]](
        getConfigFromResources(repositoryConfigFile)
      )
      .content

    val appConfig = AppConf(
      volumeMap = volumeMap,
      datasets = datasets
    )

    appConfig
  }

  // TODO: This sucks, but searching resources via pattern in Java is a pain
  private def findSources(
    index: Int = 0,
    tail: List[Dataset] = List.empty
  ): List[Dataset] = {
    val stream = getClass.getClassLoader.getResourceAsStream(
      s"dataset_$index.yaml"
    )

    if (stream == null) {
      tail.reverse
    } else {
      val ds = yaml.load[Manifest[Dataset]](stream).content

      if (ds.rootPollingSource.isEmpty)
        throw new RuntimeException(
          s"Expected a root dataset, got ${ds.kind}"
        )

      findSources(index + 1, ds :: tail)
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
