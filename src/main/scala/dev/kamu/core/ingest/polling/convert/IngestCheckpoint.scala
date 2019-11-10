package dev.kamu.core.ingest.polling.convert

import java.time.Instant

import dev.kamu.core.manifests.Resource

case class IngestCheckpoint(
  prepTimestamp: Instant,
  lastIngested: Instant
) extends Resource[IngestCheckpoint]
