package dev.kamu.core.ingest.polling

import java.io.{InputStream, OutputStream, PrintWriter}

/** Removes the poorly formatted header
  * Removes trailing commas from all lines
  */
object WorldBank {

  val HEADER_NUM_LINES = 4

  // TODO: Replace with generic options to skip N lines
  def toPlainCSV(inputStream: InputStream, outputStream: OutputStream): Unit = {
    val source = scala.io.Source.fromInputStream(inputStream)
    val writer = new PrintWriter(outputStream)

    for (line <- source.getLines.drop(HEADER_NUM_LINES)) {
      writer.write(line.replaceAll(",$", ""))
      writer.write("\n")
    }

    inputStream.close()
    source.close()
    writer.close()
  }
}
