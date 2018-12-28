import java.io.{FileInputStream, FileOutputStream}
import java.nio.file.Paths
import java.util.zip.{GZIPOutputStream, ZipEntry, ZipInputStream}
import org.apache.log4j.LogManager


case class CompressionInfo(
  isCompressed: Boolean,
  needsTranscoding: Boolean,
  currentCompression: String,
  targetCompression: String
)


case class CompressionResult(
  wasCompressed: Boolean,
  isCompressed: Boolean,
  wasTranscoded: Boolean,
  prevCompression: String,
  currentCompression: String,
  filePath: String
)


class Compression {
  private val logger = LogManager.getLogger(getClass.getName)

  def analyze(filePath: String): CompressionInfo = {
    val fileName = Paths.get(filePath).getFileName.toString

    fileName match {
      case _ if fileName.endsWith(".zip") =>
        CompressionInfo(
          isCompressed = true,
          needsTranscoding = true,
          currentCompression = "zip",
          targetCompression = "gz")
      case _ =>
        CompressionInfo(
          isCompressed = false,
          needsTranscoding = false,
          currentCompression = "",
          targetCompression = "")
    }
  }

  def process(filePath: String): CompressionResult = {
    val ci = analyze(filePath)

    ci match {
      case CompressionInfo(_, false, _, _) =>
        CompressionResult(
          wasCompressed = ci.isCompressed,
          isCompressed = ci.isCompressed,
          wasTranscoded = false,
          prevCompression = ci.currentCompression,
          currentCompression = ci.currentCompression,
          filePath = filePath)
      case CompressionInfo(_, _, "zip", "gz") =>
        logger.info(s"Transcoding file from zip to gz: $filePath")
        zip_to_gz(filePath)
      case _ =>
        throw new NotImplementedError
    }
  }

  private def zip_to_gz(filePath: String): CompressionResult = {
    val fileInStream = new FileInputStream(filePath)
    val zipInStream = new ZipInputStream(fileInStream)

    // TODO: Only supporting ZIP files with one file inside
    val outPath = filePath.replaceAll("\\.zip$", ".gz")
    val fileOutStream = new FileOutputStream(outPath)
    val gzipOutStream = new GZIPOutputStream(fileOutStream)

    Stream.continually(zipInStream.getNextEntry)
      .takeWhile(_ != null)
      .foreach { zipEntry =>
        val buffer = new Array[Byte](4096)

        Stream.continually(zipInStream.read(buffer))
          .takeWhile(_ != -1)
          .foreach(gzipOutStream.write(buffer, 0, _))

        gzipOutStream.finish()
        gzipOutStream.close()
    }

    CompressionResult(
      wasCompressed = true,
      isCompressed = true,
      wasTranscoded = true,
      prevCompression = "zip",
      currentCompression = "gz",
      filePath = outPath)
  }

}
