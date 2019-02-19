import java.util.zip.{GZIPOutputStream, ZipInputStream}

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.LogManager


class Compression(fileSystem: FileSystem) {
  private val logger = LogManager.getLogger(getClass.getName)

  def process(source: Source, inPath: Path, outPath: Path): Unit = {
    source.compression.map(_.toLowerCase) match {
      case None =>
        logger.info("Considering file uncompressed")
        compressGZip(inPath, outPath)
        fileSystem.delete(inPath, false)
      case Some("gzip") =>
        logger.info("File is already has desired compression")
        fileSystem.rename(inPath, outPath)
      case Some("zip") =>
        logger.info("Transcoding between compression formats")
        transcodeZipToGZip(inPath, source.subPath, outPath)
        fileSystem.delete(inPath, false)
      case _ =>
        throw new NotImplementedError
    }
  }

  private def compressGZip(inPath: Path, outPath: Path): Unit = {
    val fileInStream = fileSystem.open(inPath)
    val fileOutStream = fileSystem.create(outPath)
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
    val fileInStream = fileSystem.open(inPath)
    val zipInStream = new ZipInputStream(fileInStream)

    val fileOutStream = fileSystem.create(outPath)
    val gzipOutStream = new GZIPOutputStream(fileOutStream)
    var processed = false

    Stream.continually(zipInStream.getNextEntry)
      .takeWhile(_ != null)
      .filter(zipEntry => subPath.isEmpty || subPath.get == new Path(zipEntry.getName))
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
