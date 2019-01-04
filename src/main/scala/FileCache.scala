import java.io._
import java.net.URL
import java.nio.file.{Files, Path}
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.commons.io.FileUtils
import org.apache.log4j.LogManager
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization
import scalaj.http.Http


case class CacheInfo(
  url: String,
  lastModified: Option[Date],
  eTag: Option[String]
)


case class DownloadResult(
  existed: Boolean,
  wasUpToDate: Boolean,
  cacheInfo: CacheInfo
)


object FileCache {
  val dateFormat = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss zzz")
}


class FileCache() {
  private implicit val formats = Serialization.formats(NoTypeHints)

  private val logger = LogManager.getLogger(getClass.getName)

  private val lastModifiedHeaderFormat = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss zzz")

  def maybeDownload(url: URL, outPath: Path): DownloadResult = {
    logger.info(s"Requested file: $url")

    val dataDir = outPath.getParent
    val maybeStoredCacheInfo = getStoredCacheInfo(url, dataDir)
    val freshCacheInfo = getFreshCacheInfo(url)

    logger.info(s"Stored cache info: $maybeStoredCacheInfo")
    logger.info(s"Latest cache info: $freshCacheInfo")

    if(isUpToDate(maybeStoredCacheInfo, freshCacheInfo)) {
      logger.info(s"File is up to date")

      return DownloadResult(
        existed = true,
        wasUpToDate = true,
        cacheInfo = freshCacheInfo)
    }

    download(url, outPath)
    storeCacheInfo(dataDir, freshCacheInfo)

    DownloadResult(
      existed = maybeStoredCacheInfo.isDefined,
      wasUpToDate = false,
      cacheInfo = freshCacheInfo)
  }

  def download(url: URL, outPath: Path): Unit = {
    if (!Files.exists(outPath.getParent))
      Files.createDirectories(outPath.getParent)

    val outFile = new File(outPath.toString)

    logger.info(s"Downloading: url=$url, path=$outPath")
    FileUtils.copyURLToFile(url, outFile)
  }

  def getStoredCacheInfo(url: URL, dataDir: Path): Option[CacheInfo] = {
    val cachePath = getCacheInfoPath(dataDir)

    if(!Files.exists(cachePath))
      return None

    val reader = new BufferedReader(new FileReader(cachePath.toString))
    val cacheInfo = Serialization.read[CacheInfo](reader)

    if (cacheInfo.url != url.toString)
      return None

    Some(cacheInfo)
  }

  def storeCacheInfo(dataDir: Path, cacheInfo: CacheInfo): Unit = {
    val cachePath = getCacheInfoPath(dataDir)

    if (!Files.exists(cachePath.getParent))
      Files.createDirectories(cachePath.getParent)

    val writer = new BufferedWriter(new FileWriter(cachePath.toString))
    Serialization.write(cacheInfo, writer)
    writer.close()
  }

  def getFreshCacheInfo(url: URL): CacheInfo = {
    url.getProtocol match {
      case "http" | "https" =>
        getFreshCacheInfoHttp(url)
      case "file" =>
        // TODO: Security concern - debug only feature
        getFreshCacheInfoFile(url)
    }
  }

  def getFreshCacheInfoHttp(url: URL): CacheInfo = {
    logger.info(s"Fetching HEAD of $url")

    val response = Http(url.toString)
      .method("HEAD")
      .asString

    val lastModified = response
      .header("Last-Modified")
      .map(lastModifiedHeaderFormat.parse)

    val eTag = response.header("ETag")

    CacheInfo(url = url.toString, lastModified = lastModified, eTag = eTag)
  }

  def getFreshCacheInfoFile(url: URL): CacheInfo = {
    val fsFile = new File(url.toURI)
    val millis = fsFile.lastModified()
    val date = new Date(millis)
    CacheInfo(url = url.toString, lastModified = Some(date), eTag = None)
  }

  def isUpToDate(maybeStoredCacheInfo: Option[CacheInfo],
                 freshCacheInfo: CacheInfo): Boolean = {
    if (maybeStoredCacheInfo.isEmpty)
      return false

    val storedCacheInfo = maybeStoredCacheInfo.get

    if (storedCacheInfo.eTag.isDefined && freshCacheInfo.eTag.isDefined)
      return storedCacheInfo.eTag.get == freshCacheInfo.eTag.get

    if (storedCacheInfo.lastModified.isDefined && freshCacheInfo.lastModified.isDefined)
      return storedCacheInfo.lastModified.get.compareTo(freshCacheInfo.lastModified.get) >= 0

    logger.warn("Unable to determine cache validity")
    false
  }

  def getCacheInfoPath(dataDir: Path): Path = {
    dataDir
      .resolve("_metadata")
      .resolve("cacheInfo.json")
  }
}