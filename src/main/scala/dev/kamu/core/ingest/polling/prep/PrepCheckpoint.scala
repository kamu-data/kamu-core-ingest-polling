package dev.kamu.core.ingest.polling.prep

import java.time.Instant

import dev.kamu.core.manifests.Resource

case class PrepCheckpoint(
  downloadTimestamp: Instant,
  lastPrepared: Instant
) extends Resource[PrepCheckpoint]
