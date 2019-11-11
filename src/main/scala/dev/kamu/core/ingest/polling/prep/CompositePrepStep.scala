package dev.kamu.core.ingest.polling.prep
import java.io.InputStream

class CompositePrepStep(steps: Vector[PrepStep]) extends PrepStep {
  override def prepare(inputStream: InputStream): InputStream = {
    steps.foldLeft((i: InputStream) => i)((f, step) => f andThen step.prepare)(
      inputStream
    )
  }

  override def join(): Unit = {
    steps.foreach(s => s.join())
  }
}
