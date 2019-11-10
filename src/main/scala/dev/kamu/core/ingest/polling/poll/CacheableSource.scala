package dev.kamu.core.ingest.polling.poll

import java.io.InputStream

import dev.kamu.core.ingest.polling.utils.ExecutionResult
import org.apache.log4j.LogManager

trait CacheableSource {
  protected val logger = LogManager.getLogger(getClass.getName)

  def sourceID: String

  def maybeDownload(
    cacheInfo: Option[DownloadCheckpoint],
    handler: InputStream => Unit
  ): ExecutionResult[DownloadCheckpoint]
}
