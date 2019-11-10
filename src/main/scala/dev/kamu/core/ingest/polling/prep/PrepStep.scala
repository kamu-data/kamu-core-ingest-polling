package dev.kamu.core.ingest.polling.prep

import java.io.InputStream

trait PrepStep {
  def prepare(inputStream: InputStream): InputStream
}
