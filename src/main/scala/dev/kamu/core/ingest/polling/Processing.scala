package dev.kamu.core.ingest.polling

import java.io.{InputStream, OutputStream}

import dev.kamu.core.manifests.DataSourcePolling
import org.apache.commons.io.IOUtils
import org.apache.log4j.LogManager

class Processing {
  val logger = LogManager.getLogger(getClass.getName)

  def process(source: DataSourcePolling,
              inputStream: InputStream,
              outputStream: OutputStream): Unit = {
    source.format.toLowerCase match {
      case "geojson" =>
        logger.info(s"Pre-processing as GeoJSON")
        GeoJSON.toMultiLineJSON(inputStream, outputStream)
      case "worldbank-csv" =>
        logger.info(s"Pre-processing as WorldBankCSV")
        WorldBank.toPlainCSV(inputStream, outputStream)
      case _ =>
        processNoOp(inputStream, outputStream)
    }
  }

  private def processNoOp(inputStream: InputStream,
                          outputStream: OutputStream): Unit = {
    IOUtils.copy(inputStream, outputStream)
    inputStream.close()
    outputStream.close()
  }
}
