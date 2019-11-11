package dev.kamu.core.ingest.polling.prep

import java.io.InputStream

import org.apache.log4j.{LogManager, Logger}

trait PrepStep {
  protected val logger: Logger = LogManager.getLogger(getClass.getName)

  def prepare(inputStream: InputStream): InputStream

  def join(): Unit = {}
}
