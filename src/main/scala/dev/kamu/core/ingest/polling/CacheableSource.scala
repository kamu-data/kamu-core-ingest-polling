package dev.kamu.core.ingest.polling

import java.io.InputStream
import java.net.URI
import java.text.SimpleDateFormat
import java.time.format.DateTimeFormatter
import java.time.{Instant, ZonedDateTime}

import org.apache.commons.net.ftp.{FTP, FTPClient}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.LogManager
import scalaj.http.Http
import dev.kamu.core.manifests.Resource

case class CacheInfo(
  url: URI,
  lastModified: Option[Instant] = None,
  eTag: Option[String] = None,
  lastDownloaded: Instant
) extends Resource[CacheInfo] {
  def isCacheable: Boolean = {
    eTag.isDefined || lastModified.isDefined
  }
}

case class DownloadResult(
  wasUpToDate: Boolean,
  cacheInfo: CacheInfo
)

abstract class CacheableSource {
  protected val logger = LogManager.getLogger(getClass.getName)

  def maybeDownload(
    url: URI,
    cacheInfo: Option[CacheInfo],
    handler: InputStream => Unit
  ): DownloadResult
}

class FileSystemCacheableSource(fileSystem: FileSystem)
    extends CacheableSource {
  override def maybeDownload(
    url: URI,
    cacheInfo: Option[CacheInfo],
    handler: InputStream => Unit
  ): DownloadResult = {
    val sourcePath = new Path(url)
    val fs = sourcePath.getFileSystem(fileSystem.getConf)

    // TODO: add cacheability via hashing or file stats
    handler(fs.open(sourcePath))

    DownloadResult(
      wasUpToDate = false,
      cacheInfo = CacheInfo(url = url, lastDownloaded = Instant.now())
    )
  }
}

class HTTPCacheableSource extends CacheableSource {
  private val lastModifiedHeaderFormat = new SimpleDateFormat(
    "EEE, dd MMM yyyy HH:mm:ss zzz"
  )

  override def maybeDownload(
    url: URI,
    cacheInfo: Option[CacheInfo],
    handler: InputStream => Unit
  ): DownloadResult = {
    var request = Http(url.toString)
      .method("GET")

    if (cacheInfo.isDefined) {
      val ci = cacheInfo.get

      if (ci.eTag.isDefined)
        request = request
          .header("If-None-Match", ci.eTag.get)

      if (ci.lastModified.isDefined)
        request = request
          .header(
            "If-Modified-Since",
            lastModifiedHeaderFormat.format(ci.lastModified)
          )
    }

    // TODO: this will write body even in case of error
    val response = request.exec((code, _, bodyStream) => {
      if (code == 200)
        handler(bodyStream)
    })

    response.code match {
      case 200 =>
        DownloadResult(
          wasUpToDate = false,
          CacheInfo(
            url = url,
            lastModified = response
              .header("LastModified")
              .map(
                s =>
                  ZonedDateTime
                    .parse(s, DateTimeFormatter.RFC_1123_DATE_TIME)
                    .toInstant
              ),
            eTag = response.header("ETag"),
            lastDownloaded = Instant.now()
          )
        )
      case 304 =>
        DownloadResult(wasUpToDate = true, cacheInfo.get)
      case _ =>
        throw new RuntimeException(s"Request failed: ${response.statusLine}")
    }
  }
}

class FTPCacheableSource extends CacheableSource {
  override def maybeDownload(
    url: URI,
    cacheInfo: Option[CacheInfo],
    handler: InputStream => Unit
  ): DownloadResult = {
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

    DownloadResult(
      wasUpToDate = false,
      cacheInfo = CacheInfo(url = url, lastDownloaded = Instant.now())
    )
  }
}
