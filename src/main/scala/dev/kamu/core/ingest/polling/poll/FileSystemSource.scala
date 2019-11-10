package dev.kamu.core.ingest.polling.poll

import java.io.InputStream
import java.net.URI
import java.time.Instant

import dev.kamu.core.ingest.polling.utils.ExecutionResult
import org.apache.hadoop.fs.{FileSystem, Path}

class FileSystemSource(fileSystem: FileSystem, url: URI)
    extends CacheableSource {

  override def sourceID: String = url.toString

  override def maybeDownload(
    cacheInfo: Option[DownloadCheckpoint],
    handler: InputStream => Unit
  ): ExecutionResult[DownloadCheckpoint] = {
    logger.info(s"FS read $url")

    val sourcePath = new Path(url)
    val fs = sourcePath.getFileSystem(fileSystem.getConf)

    val lastModified =
      Instant.ofEpochMilli(fs.getFileStatus(sourcePath).getModificationTime)

    val needsPull = cacheInfo
      .flatMap(_.lastModified)
      .forall(lastModified.compareTo(_) > 0)

    if (needsPull) {
      handler(fs.open(sourcePath))

      ExecutionResult(
        wasUpToDate = false,
        checkpoint = DownloadCheckpoint(
          lastDownloaded = Instant.now(),
          lastModified = Some(lastModified)
        )
      )
    } else {
      ExecutionResult(
        wasUpToDate = true,
        checkpoint = cacheInfo.get
      )
    }
  }
}
