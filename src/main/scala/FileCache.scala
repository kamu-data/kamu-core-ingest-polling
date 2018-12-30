import java.io._
import java.net.URL
import java.nio.file.{Files, Path, Paths}
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.commons.io.FileUtils
import org.apache.log4j.LogManager
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization
import scalaj.http.Http


case class CacheInfo(
  lastModified: Option[Date],
  eTag: Option[String]
)

class CachedFile(
  val namespace: String,
  val url: URL
) {
  val fileName: String = Paths.get(url.getPath).getFileName.toString
}

case class DownloadResult(
  existed: Boolean,
  wasUpToDate: Boolean,
  filePath: String,
  cacheInfo: CacheInfo
)


object FileCache {
  val dateFormat = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss zzz")
}


class FileCache(val downloadsDir: Path) {
  private implicit val formats = Serialization.formats(NoTypeHints)

  private val logger = LogManager.getLogger(getClass.getName)

  private val lastModifiedHeaderFormat = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss zzz")

  def maybeDownload(file: CachedFile): DownloadResult = {
    logger.info(s"Requested file: ${file.url}")

    val maybeStoredCacheInfo = getStoredCacheInfo(file)
    val freshCacheInfo = getFreshCacheInfo(file)

    logger.info(s"Stored cache info: $maybeStoredCacheInfo")
    logger.info(s"Latest cache info: $freshCacheInfo")

    if(isUpToDate(maybeStoredCacheInfo, freshCacheInfo)) {
      logger.info(s"File ${file.fileName} is up to date")

      return DownloadResult(
        existed = true,
        wasUpToDate = true,
        filePath = getDownloadPath(file).toString,
        cacheInfo = freshCacheInfo)
    }

    download(file)
    storeCacheInfo(file, freshCacheInfo)

    DownloadResult(
      existed = maybeStoredCacheInfo.isDefined,
      wasUpToDate = false,
      filePath = getDownloadPath(file).toString,
      cacheInfo = freshCacheInfo)
  }

  def download(file: CachedFile): Unit = {
    val downloadPath = getDownloadPath(file)

    if (!Files.exists(downloadPath.getParent))
      Files.createDirectories(downloadPath.getParent)

    val downloadFile = new File(downloadPath.toString)

    logger.info(s"Downloading: url=${file.url}, path=$downloadPath")
    FileUtils.copyURLToFile(file.url, downloadFile)
  }

  def getStoredCacheInfo(file: CachedFile): Option[CacheInfo] = {
    val cachePath = getCacheInfoPath(file)

    if(!Files.exists(cachePath))
      return None

    val reader = new BufferedReader(new FileReader(cachePath.toString))
    val cacheInfo = Serialization.read[CacheInfo](reader)

    Some(cacheInfo)
  }

  def storeCacheInfo(file: CachedFile, cacheInfo: CacheInfo): Unit = {
    val cachePath = getCacheInfoPath(file)

    if (!Files.exists(cachePath.getParent))
      Files.createDirectories(cachePath.getParent)

    val writer = new BufferedWriter(new FileWriter(cachePath.toString))
    Serialization.write(cacheInfo, writer)
    writer.close()
  }

  def getFreshCacheInfo(file: CachedFile): CacheInfo = {
    file.url.getProtocol match {
      case "http" | "https" =>
        getFreshCacheInfoHttp(file)
      case "file" =>
        // TODO: Security concern - debug only feature
        getFreshCacheInfoFile(file)
    }
  }

  def getFreshCacheInfoHttp(file: CachedFile): CacheInfo = {
    logger.info(s"Fetching HEAD of ${file.url}")

    val response = Http(file.url.toString)
      .method("HEAD")
      .asString

    val lastModified = response
      .header("Last-Modified")
      .map(lastModifiedHeaderFormat.parse)

    val eTag = response.header("ETag")

    CacheInfo(lastModified, eTag)
  }

  def getFreshCacheInfoFile(file: CachedFile): CacheInfo = {
    val fsFile = new File(file.url.toURI)
    val millis = fsFile.lastModified()
    val date = new Date(millis)
    CacheInfo(lastModified = Some(date), eTag = None)
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

  def getCacheInfoPath(file: CachedFile): Path = {
    downloadsDir
      .resolve(file.namespace)
      .resolve("_metadata")
      .resolve(file.fileName + ".json")
  }

  def getDownloadPath(file: CachedFile): Path = {
    downloadsDir.resolve(file.namespace).resolve(file.fileName)
  }
}