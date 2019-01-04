import java.io.{FileInputStream, FileOutputStream}
import java.nio.file.{Files, Path, Paths}
import java.util.zip.{GZIPOutputStream, ZipInputStream}

import org.apache.log4j.LogManager


class Compression {
  private val logger = LogManager.getLogger(getClass.getName)

  def process(source: Source, inPath: Path, outPath: Path): Unit = {
    source.compression.map(_.toLowerCase) match {
      case None =>
        logger.info("Considering file uncompressed")
        compressGZip(inPath, outPath)
        Files.delete(inPath)
      case Some("gzip") =>
        logger.info("File is already has desired compression")
        Files.move(inPath, outPath)
      case Some("zip") =>
        logger.info("Transcoding between compression formats")
        transcodeZipToGZip(inPath, source.subPath, outPath)
        Files.delete(inPath)
      case _ =>
        throw new NotImplementedError
    }
  }

  private def compressGZip(inPath: Path, outPath: Path): Unit = {
    val fileInStream = new FileInputStream(inPath.toString)
    val fileOutStream = new FileOutputStream(outPath.toString)
    val gzipOutStream = new GZIPOutputStream(fileOutStream)

    val buffer = new Array[Byte](4096)

    Stream.continually(fileInStream.read(buffer))
      .takeWhile(_ != -1)
      .foreach(gzipOutStream.write(buffer, 0, _))

    gzipOutStream.finish()
    gzipOutStream.close()
  }

  private def transcodeZipToGZip(
      inPath: Path, subPath: Option[Path], outPath: Path): Unit = {
    val fileInStream = new FileInputStream(inPath.toString)
    val zipInStream = new ZipInputStream(fileInStream)

    val fileOutStream = new FileOutputStream(outPath.toString)
    val gzipOutStream = new GZIPOutputStream(fileOutStream)
    var processed = false

    Stream.continually(zipInStream.getNextEntry)
      .takeWhile(_ != null)
      .filter(zipEntry => subPath.isEmpty || subPath.get == Paths.get(zipEntry.getName))
      .foreach { zipEntry =>
        if (processed)
          throw new RuntimeException(
            "Zip file has multiple entries but the subPath was not specified")

        logger.info(s"Extracting zip entry: ${zipEntry.getName}")

        val buffer = new Array[Byte](4096)

        Stream.continually(zipInStream.read(buffer))
          .takeWhile(_ != -1)
          .foreach(gzipOutStream.write(buffer, 0, _))

        gzipOutStream.finish()
        gzipOutStream.close()
        processed = true
    }

    if (!processed)
      throw new RuntimeException("Failed to find an entry in the Zip file")
  }

}
