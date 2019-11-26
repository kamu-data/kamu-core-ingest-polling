/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.core.ingest.polling.poll

import java.io.InputStream
import java.net.URI
import java.time.Instant

import dev.kamu.core.ingest.polling.utils.ExecutionResult
import org.apache.commons.net.ftp.{FTP, FTPClient}

class FTPSource(url: URI) extends CacheableSource {

  override def sourceID: String = url.toString

  override def maybeDownload(
    checkpoint: Option[DownloadCheckpoint],
    cachingBehavior: CachingBehavior,
    handler: InputStream => Unit
  ): ExecutionResult[DownloadCheckpoint] = {
    if (!cachingBehavior.shouldDownload(checkpoint))
      return ExecutionResult(
        wasUpToDate = true,
        checkpoint = checkpoint.get
      )

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
      checkpoint = SimpleDownloadCheckpoint(lastDownloaded = Instant.now())
    )
  }
}