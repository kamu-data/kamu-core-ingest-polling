import java.io._
import java.net.URI
import java.text.SimpleDateFormat
import java.util.Date

import FSUtils._
import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.LogManager
import org.json4s.JsonAST.{JNull, JString}
import org.json4s.jackson.Serialization
import org.json4s.{CustomSerializer, NoTypeHints}
import scalaj.http._


case class CacheInfo(
  url: URI,
  lastModified: Option[Date],
  eTag: Option[String],
  lastDownloadDate: Date
) {
  def isCacheable: Boolean = {
    eTag.isDefined || lastModified.isDefined
  }
}


case class DownloadResult(
  wasUpToDate: Boolean,
  cacheInfo: CacheInfo
)


class FileCache(fileSystem: FileSystem) {
  private implicit val formats = Serialization.formats(NoTypeHints) + UriSerializer

  private val logger = LogManager.getLogger(getClass.getName)

  private val lastModifiedHeaderFormat = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss zzz")

  def maybeDownload(url: URI, outPath: Path, cacheDir: Path): DownloadResult = {
    logger.info(s"Requested file: $url")

    if (!fileSystem.exists(outPath.getParent))
      fileSystem.mkdirs(outPath.getParent)

    val maybeStoredCacheInfo = getStoredCacheInfo(url, cacheDir)
    val outFile = new File(outPath.toString)

    var request = Http(url.toString)
      .method("GET")

    if(maybeStoredCacheInfo.isDefined) {
      val cacheInfo = maybeStoredCacheInfo.get
      logger.info(s"Cache info: $cacheInfo")

      if (!cacheInfo.isCacheable) {
        logger.warn(s"Skipping uncachable source")
        return DownloadResult(wasUpToDate = true, cacheInfo)
      }

      if(cacheInfo.eTag.isDefined)
        request = request
          .header("If-None-Match", cacheInfo.eTag.get)

      if(cacheInfo.lastModified.isDefined)
        request = request
          .header("If-Modified-Since", lastModifiedHeaderFormat.format(cacheInfo.lastModified))
    } else {
      logger.info("Fist time download")
    }

    // TODO: this will write body even in case of
    val response = request.execute(bodyStream => {
      val outputStream = fileSystem.create(outPath)
      IOUtils.copy(bodyStream, outputStream)
      bodyStream.close()
      outputStream.close()
    })

    if(response.code == 304) {
      logger.info("Data is up to date")

      return DownloadResult(wasUpToDate = true, maybeStoredCacheInfo.get)
    }
    else if(!response.is2xx) {
      throw new RuntimeException(
        s"Request failed: ${response.statusLine}")
    }

    val freshCacheInfo = CacheInfo(
      url = url,
      lastModified = response.header("LastModified").map(lastModifiedHeaderFormat.parse),
      eTag = response.header("ETag"),
      lastDownloadDate = new Date())

    if (!freshCacheInfo.isCacheable)
      logger.warn(
        s"Response for URL $url is uncacheable. " +
        s"Data will not be updated automatically.")

    storeCacheInfo(freshCacheInfo, cacheDir)

    DownloadResult(wasUpToDate = false, freshCacheInfo)
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
