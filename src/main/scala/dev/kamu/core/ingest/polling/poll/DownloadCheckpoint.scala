package dev.kamu.core.ingest.polling.poll

import java.time.Instant

import dev.kamu.core.manifests.Resource

case class DownloadCheckpoint(
  lastModified: Option[Instant] = None,
  eTag: Option[String] = None,
  lastDownloaded: Instant
) extends Resource[DownloadCheckpoint] {
  def isCacheable: Boolean = {
    eTag.isDefined || lastModified.isDefined
  }
}
