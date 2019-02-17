import java.io._
import java.net.URL
import java.nio.file.{Files, Path}
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.log4j.LogManager
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization
import com.softwaremill.sttp._


case class CacheInfo(
  url: String,
  lastModified: Option[Date],
  eTag: Option[String],
  lastDownloadDate: Date
)


case class DownloadResult(
  wasUpToDate: Boolean,
  cacheInfo: CacheInfo
)


class FileCache() {
  private implicit val formats = Serialization.formats(NoTypeHints)
  private implicit val backend = HttpURLConnectionBackend()

  private val logger = LogManager.getLogger(getClass.getName)

  private val lastModifiedHeaderFormat = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss zzz")

  def maybeDownload(url: URL, outPath: Path, cacheDir: Path): DownloadResult = {
    logger.info(s"Requested file: $url")

    if (!Files.exists(outPath.getParent))
      Files.createDirectories(outPath.getParent)

    val maybeStoredCacheInfo = getStoredCacheInfo(url, cacheDir)
    val outFile = new File(outPath.toString)

    var requestBuilder = sttp
      .response(asFile(outFile, true))

    if(maybeStoredCacheInfo.isDefined) {
      val cacheInfo = maybeStoredCacheInfo.get
      logger.info(s"Cache info: $cacheInfo")

      if(cacheInfo.eTag.isDefined)
        requestBuilder = requestBuilder
          .header("If-None-Match", cacheInfo.eTag.get)

      if(cacheInfo.lastModified.isDefined)
        requestBuilder = requestBuilder
          .header("If-Modified-Since", lastModifiedHeaderFormat.format(cacheInfo.lastModified))
    } else {
      logger.info("Fist time download")
    }

    val request = requestBuilder
      .get(Uri(url.toURI))

    val response = request.send()

    if(response.code == 304) {
      logger.info("Data is up to date")

      return DownloadResult(
        wasUpToDate = true,
        cacheInfo = maybeStoredCacheInfo.get)
    }
    else if(!response.is200) {
      throw new RuntimeException(
        s"Request failed: ${response.code} ${response.statusText}")
    }

    val freshCacheInfo = CacheInfo(
      url = url.toString,
      lastModified = response.header("LastModified").map(lastModifiedHeaderFormat.parse),
      eTag = response.header("ETag"),
      lastDownloadDate = new Date())

    storeCacheInfo(freshCacheInfo, cacheDir)
    DownloadResult(
      wasUpToDate = false,
      cacheInfo = freshCacheInfo)
  }

  def getStoredCacheInfo(url: URL, cacheDir: Path): Option[CacheInfo] = {
    val cachePath = getCacheInfoPath(cacheDir)

    if(!Files.exists(cachePath))
      return None

    val reader = new BufferedReader(new FileReader(cachePath.toString))
    val cacheInfo = Serialization.read[CacheInfo](reader)

    if (cacheInfo.url != url.toString)
      return None

    Some(cacheInfo)
  }

  def storeCacheInfo(cacheInfo: CacheInfo, cacheDir: Path): Unit = {
    val cachePath = getCacheInfoPath(cacheDir)

    if (!Files.exists(cachePath.getParent))
      Files.createDirectories(cachePath.getParent)

    val writer = new BufferedWriter(new FileWriter(cachePath.toString))
    Serialization.write(cacheInfo, writer)
    writer.close()
  }

  def getCacheInfoPath(cacheDir: Path): Path = {
    cacheDir
      .resolve("cacheInfo.json")
  }
}