package dev.kamu.core.ingest.polling

import java.io.InputStream

import dev.kamu.core.manifests.{Dataset, Manifest, Resource}
import org.apache.hadoop.fs.Path

case class IngestTask(
  datasetToIngest: Dataset,
  checkpointsPath: Path,
  pollCachePath: Path,
  dataPath: Path
) extends Resource[IngestTask]

case class AppConf(
  tasks: Vector[IngestTask]
) extends Resource[AppConf]

object AppConf {
  import dev.kamu.core.manifests.parsing.pureconfig.yaml
  import yaml.defaults._
  import pureconfig.generic.auto._

  val configFileName = "pollConfig.yaml"
  val downloadCheckpointFileName = "download.checkpoint.yaml"
  val downloadDataFileName = "download.bz2"
  val prepCheckpointFileName = "prepare.checkpoint.yaml"
  val prepDataFileName = "prepare.bz2"
  val ingestCheckpointFileName = "ingest.checkpoint.yaml"

  def load(): AppConf = {
    yaml
      .load[Manifest[AppConf]](getConfigFromResources(configFileName))
      .content
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
