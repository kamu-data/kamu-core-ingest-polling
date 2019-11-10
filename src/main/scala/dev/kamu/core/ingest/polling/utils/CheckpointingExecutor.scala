package dev.kamu.core.ingest.polling.utils

import dev.kamu.core.manifests.parsing.pureconfig.yaml
import dev.kamu.core.manifests.{Manifest, Resource}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.LogManager
import pureconfig.{ConfigReader, ConfigWriter, Derivation}

case class ExecutionResult[TCheckpoint](
  wasUpToDate: Boolean,
  checkpoint: TCheckpoint
)

class CheckpointingExecutor[TCheckpoint <: Resource[TCheckpoint]](
  fileSystem: FileSystem
)(
  implicit icr: Derivation[ConfigReader[TCheckpoint]],
  icmr: Derivation[ConfigReader[Manifest[TCheckpoint]]],
  icw: Derivation[ConfigWriter[TCheckpoint]],
  icmw: Derivation[ConfigWriter[Manifest[TCheckpoint]]]
) {
  private val logger = LogManager.getLogger(getClass.getName)

  def execute(
    checkpointPath: Path,
    execute: Option[TCheckpoint] => ExecutionResult[TCheckpoint]
  ): ExecutionResult[TCheckpoint] = {
    val storedCheckpoint = readCheckpoint(checkpointPath)

    if (storedCheckpoint.isDefined) {
      logger.info(s"Stored checkpoint: ${storedCheckpoint.get}")
    } else {
      logger.info("Fist time pass")
    }

    val executionResult = execute(storedCheckpoint)

    if (executionResult.wasUpToDate) {
      logger.info("Up to date")
    } else {
      logger.info("Saving checkpoint")
      writeCheckpoint(checkpointPath, executionResult.checkpoint)
    }

    executionResult
  }

  def readCheckpoint(
    checkpointPath: Path
  ): Option[TCheckpoint] = {
    if (!fileSystem.exists(checkpointPath))
      return None

    val inputStream = fileSystem.open(checkpointPath)
    val cacheInfo = yaml.load[Manifest[TCheckpoint]](inputStream).content

    // TODO: detect when cache should be invalidated
    Some(cacheInfo)
  }

  def writeCheckpoint(checkpointPath: Path, checkpoint: TCheckpoint): Unit = {
    if (!fileSystem.exists(checkpointPath.getParent))
      fileSystem.mkdirs(checkpointPath.getParent)

    val outputStream = fileSystem.create(checkpointPath)

    yaml.save(checkpoint.asManifest, outputStream)
    outputStream.close()
  }
}
