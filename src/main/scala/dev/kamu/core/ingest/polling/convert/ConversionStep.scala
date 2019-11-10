package dev.kamu.core.ingest.polling.convert

import java.io.InputStream

trait ConversionStep {
  def convert(input: InputStream): InputStream
}

class NoOpConversionStep extends ConversionStep {
  override def convert(input: InputStream): InputStream = input
}
