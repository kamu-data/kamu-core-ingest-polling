package dev.kamu.core.ingest.polling

import java.io.{InputStream, OutputStream}
import java.util.regex.Pattern
import java.util.zip.GZIPInputStream

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream
import org.apache.hadoop.fs.FileSystem
import org.apache.log4j.LogManager

class Compression(fileSystem: FileSystem) {
  private val logger = LogManager.getLogger(getClass.getName)

  def getExtractedStream(source: SourceConf,
                         inputStream: InputStream): InputStream = {
    source.compression.map(_.toLowerCase) match {
      case None =>
        logger.info("Considering file uncompressed")
        inputStream
      case Some("gzip") =>
        logger.info("Extracting gzip")
        new GZIPInputStream(inputStream)
      case Some("zip") =>
        logger.info("Extracting zip")

        val subPathRegex = source.subPathRegex.orElse(source.subPath.map(p =>
          Pattern.quote(p.toString)))

        val stream = subPathRegex
          .map(
            regex =>
              ZipEntryStream
                .findFirst(inputStream, regex)
                .getOrElse(throw new RuntimeException(
                  "Failed to find an entry in the Zip file")))
          .getOrElse(
            ZipEntryStream.first(inputStream)
          )

        logger.info(s"Picking Zip entry ${stream.entry.getName}")
        stream
      case _ =>
        throw new NotImplementedError
    }
  }

  val fileExtension = "bz2"

  def toCompressedStream(outputStream: OutputStream): OutputStream = {
    new BZip2CompressorOutputStream(outputStream)
  }

}
