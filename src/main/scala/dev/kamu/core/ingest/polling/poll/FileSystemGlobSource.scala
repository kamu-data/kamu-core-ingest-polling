/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.core.ingest.polling.poll

import java.io.InputStream
import java.time.Instant

import dev.kamu.core.ingest.polling.utils.ExecutionResult
import org.apache.hadoop.fs.{FileSystem, Path}

class FileSystemGlobSource(fileSystem: FileSystem, pathPattern: Path)
    extends CacheableSource {

  override def sourceID: String = pathPattern.toString

  override def canProduceMultipleResults = true

  override def maybeDownload(
    checkpoint: Option[DownloadCheckpoint],
    cachingBehavior: CachingBehavior,
    handler: InputStream => Unit
  ): ExecutionResult[DownloadCheckpoint] = {
    logger.info(s"FS glob $pathPattern")

    val globbedFiles = fileSystem
      .globStatus(pathPattern)
      .map(_.getPath)
      .sortBy(_.toString)

    val oldCheckpoint = checkpoint
      .map(_.asInstanceOf[MultiSourceDownloadCheckpoint])
      .getOrElse(MultiSourceDownloadCheckpoint(lastDownloaded = Instant.now()))

    val childCheckpoints = oldCheckpoint.children
      .map(c => c.source -> c)
      .toMap

    for (file <- globbedFiles) {
      val childCheckpoint = childCheckpoints
        .get(file.toString)
        .map(c => c.checkpoint)

      val fileSource = new FileSystemSource(fileSystem, file)

      val result = fileSource.maybeDownload(
        childCheckpoint,
        cachingBehavior,
        handler
      )

      if (!result.wasUpToDate) {
        val newChildCheckpoint = MultiSourceDownloadCheckpoint
          .ChildCheckpoint(
            source = file.toString,
            checkpoint =
              result.checkpoint.asInstanceOf[SimpleDownloadCheckpoint]
          )

        val updatedChildCheckpoints = (
          childCheckpoints + (file.toString -> newChildCheckpoint)
        ).toVector.sortBy(_._1).map(_._2)

        return ExecutionResult(
          wasUpToDate = false,
          checkpoint = MultiSourceDownloadCheckpoint(
            lastDownloaded = result.checkpoint.lastDownloaded,
            children = updatedChildCheckpoints
          )
        )
      }
    }

    ExecutionResult(
      wasUpToDate = true,
      checkpoint = oldCheckpoint
    )
  }
}
