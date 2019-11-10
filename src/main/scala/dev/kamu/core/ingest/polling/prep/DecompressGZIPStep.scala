package dev.kamu.core.ingest.polling.prep
import java.io.InputStream
import java.util.zip.GZIPInputStream

class DecompressGZIPStep extends PrepStep {
  override def prepare(inputStream: InputStream): InputStream = {
    new GZIPInputStream(inputStream)
  }
}
