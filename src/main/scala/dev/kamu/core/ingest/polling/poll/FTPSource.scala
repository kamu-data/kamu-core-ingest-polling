package dev.kamu.core.ingest.polling.poll

import java.io.InputStream
import java.net.URI
import java.time.Instant

import dev.kamu.core.ingest.polling.utils.ExecutionResult
import org.apache.commons.net.ftp.{FTP, FTPClient}

class FTPSource(url: URI) extends CacheableSource {

  override def sourceID: String = url.toString

  override def maybeDownload(
    cacheInfo: Option[DownloadCheckpoint],
    handler: InputStream => Unit
  ): ExecutionResult[DownloadCheckpoint] = {
    logger.info(s"FTP request $url")

    val client = new FTPClient()
    client.connect(url.getHost)
    client.login("anonymous", "")
    client.enterLocalPassiveMode()
    client.setFileType(FTP.BINARY_FILE_TYPE)

    val inputStream = client.retrieveFileStream(url.getPath)
    handler(inputStream)

    val success = client.completePendingCommand()
    inputStream.close()

    if (!success)
      throw new RuntimeException(s"FTP download failed")

    ExecutionResult(
      wasUpToDate = false,
      checkpoint = DownloadCheckpoint(lastDownloaded = Instant.now())
    )
  }
}
