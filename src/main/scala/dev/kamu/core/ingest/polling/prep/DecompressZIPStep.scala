package dev.kamu.core.ingest.polling.prep

import java.io.InputStream
import java.util.regex.Pattern

import dev.kamu.core.ingest.polling.utils.ZipEntryStream
import dev.kamu.core.manifests.PrepStepDecompress
import org.apache.log4j.LogManager

class DecompressZIPStep(config: PrepStepDecompress) extends PrepStep {
  private val logger = LogManager.getLogger(getClass.getName)

  override def prepare(inputStream: InputStream): InputStream = {
    val subPathRegex = config.subPathRegex.orElse(
      config.subPath.map(p => Pattern.quote(p.toString))
    )

    val stream = subPathRegex
      .map(
        regex =>
          ZipEntryStream
            .findFirst(inputStream, regex)
            .getOrElse(
              throw new RuntimeException(
                "Failed to find an entry in the Zip file"
              )
            )
      )
      .getOrElse(
        ZipEntryStream.first(inputStream)
      )

    logger.info(s"Picking Zip entry ${stream.entry.getName}")
    stream
  }
}
