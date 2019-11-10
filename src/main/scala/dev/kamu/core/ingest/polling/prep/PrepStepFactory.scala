package dev.kamu.core.ingest.polling.prep

import java.io.InputStream

import dev.kamu.core.manifests.{PrepStepDecompress, PrepStepKind}
import org.apache.hadoop.fs.FileSystem
import org.apache.log4j.LogManager

class PrepStepFactory(fileSystem: FileSystem) {
  private val logger = LogManager.getLogger(getClass.getName)

  def getStep(
    config: PrepStepKind
  ): PrepStep = {
    config match {
      case dc: PrepStepDecompress =>
        dc.format.toLowerCase match {
          case "gzip" =>
            logger.info("Extracting gzip")
            new DecompressGZIPStep()
          case "zip" =>
            logger.info("Extracting zip")
            new DecompressZIPStep(dc)
          case _ =>
            throw new NotImplementedError(
              s"Unknown compression format: ${dc.format}"
            )
        }
      case _ =>
        throw new NotImplementedError(s"Unknown prep step: $config")
    }
  }

  def getComposedSteps(
    configs: Seq[PrepStepKind]
  ): InputStream => InputStream = {
    val steps = configs.map(getStep)
    val noop = (i: InputStream) => i
    steps.foldLeft(noop)((f, step) => f andThen step.prepare)
  }
}
