package dev.kamu.core.ingest.polling

import java.io.{FilterInputStream, InputStream}
import java.util.zip.{ZipEntry, ZipInputStream}

object ZipEntryStream {
  def first(inputStream: InputStream): ZipEntryStream = {
    val zipStream = new ZipInputStream(inputStream)
    new ZipEntryStream(zipStream, zipStream.getNextEntry)
  }

  def findFirst(inputStream: InputStream,
                regex: String): Option[ZipEntryStream] = {
    val zipStream = new ZipInputStream(inputStream)
    var entry = zipStream.getNextEntry
    while (entry != null) {
      if (entry.getName.matches(regex))
        return Some(new ZipEntryStream(zipStream, entry))
      entry = zipStream.getNextEntry
    }
    None
  }
}

class ZipEntryStream private (
  val zipStream: ZipInputStream,
  val entry: ZipEntry
) extends FilterInputStream(zipStream)
