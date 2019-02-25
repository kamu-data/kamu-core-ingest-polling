import java.io._
import java.net.URI

import FSUtils._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.LogManager
import org.json4s.JsonAST.{JNull, JString}
import org.json4s.jackson.Serialization
import org.json4s.{CustomSerializer, NoTypeHints}


class CachingDownloader(fileSystem: FileSystem) {
  private implicit val formats = Serialization.formats(NoTypeHints) + UriSerializer
  private val logger = LogManager.getLogger(getClass.getName)

  def maybeDownload(url: URI, cacheDir: Path, handler: InputStream => Unit): DownloadResult = {
    logger.info(s"Requested file: $url")

    val storedCacheInfo = getStoredCacheInfo(url, cacheDir)
    if (storedCacheInfo.isDefined) {
      logger.info(s"Stored cache info: ${storedCacheInfo.get}")
      if (!storedCacheInfo.get.isCacheable) {
        logger.warn(s"Skipping uncachable source")
        return DownloadResult(wasUpToDate = true, storedCacheInfo.get)
      }
    } else {
      logger.info("Fist time download")
    }

    val source = getSource(url)
    val downloadResult = source.maybeDownload(url, storedCacheInfo, handler)

    if (downloadResult.wasUpToDate) {
      logger.info("Data is up to date")
    } else {
      if (!downloadResult.cacheInfo.isCacheable)
        logger.warn(
          s"Response for URL $url is uncacheable. " +
          s"Data will not be updated automatically.")

      storeCacheInfo(downloadResult.cacheInfo, cacheDir)
    }

    downloadResult
  }

  def getSource(url: URI): CacheableSource = {
    url.getScheme match {
      case "http" | "https" =>
        new HTTPCacheableSource()
      case "gs" =>
        new FileSystemCacheableSource(fileSystem)
      case "hdfs" | null =>
        // TODO: restrict allowed source paths for security
        new FileSystemCacheableSource(fileSystem)
      case _ =>
        throw new NotImplementedError(s"Unsupported source: $url")
    }
  }

  def getStoredCacheInfo(url: URI, cacheDir: Path): Option[CacheInfo] = {
    val cachePath = getCacheInfoPath(cacheDir)

    if (!fileSystem.exists(cachePath))
      return None

    val inputStream = fileSystem.open(cachePath)
    val cacheInfo = Serialization.read[CacheInfo](inputStream)

    if (cacheInfo.url != url)
      return None

    Some(cacheInfo)
  }

  def storeCacheInfo(cacheInfo: CacheInfo, cacheDir: Path): Unit = {
    val cachePath = getCacheInfoPath(cacheDir)

    if (!fileSystem.exists(cachePath.getParent))
      fileSystem.mkdirs(cachePath.getParent)

    val outputStream = fileSystem.create(cachePath)
    Serialization.write(cacheInfo, outputStream)
    outputStream.close()
  }

  def getCacheInfoPath(cacheDir: Path): Path = {
    cacheDir.resolve(AppConfig.pollCacheFileName)
  }
}


case object UriSerializer extends CustomSerializer[URI](
  format => ({
    case JString(uri) => URI.create(uri)
    case JNull => null
  }, {
    case uri: URI => JString(uri.toString)
  })
)
