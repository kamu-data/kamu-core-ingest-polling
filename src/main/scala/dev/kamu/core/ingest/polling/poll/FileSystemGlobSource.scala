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

class FileSystemGlobSource(
  fileSystem: FileSystem,
  pathPattern: Path,
  eventTimeSource: EventTimeSource
) extends CacheableSource {

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

    val oldCheckpoint = checkpoint
      .map(_.asInstanceOf[MultiSourceDownloadCheckpoint])
      .getOrElse(
        MultiSourceDownloadCheckpoint(
          lastDownloaded = Instant.now(),
          eventTime = None
        )
      )

    val childCheckpoints = oldCheckpoint.children
      .map(c => c.source -> c)
      .toMap

    val sources = globbedFiles
      .map(new FileSystemSource(fileSystem, _, eventTimeSource))
      .sortBy(s => eventTimeSource.getEventTime(s))

    for (fileSource <- sources) {
      val childCheckpoint = childCheckpoints
        .get(fileSource.path.toString)
        .map(c => c.checkpoint)

      val result = fileSource.maybeDownload(
        childCheckpoint,
        cachingBehavior,
        handler
      )

      if (!result.wasUpToDate) {
        val newChildCheckpoint = MultiSourceDownloadCheckpoint
          .ChildCheckpoint(
            source = fileSource.path.toString,
            checkpoint =
              result.checkpoint.asInstanceOf[SimpleDownloadCheckpoint]
          )

        val updatedChildCheckpoints = (
          childCheckpoints + (fileSource.path.toString -> newChildCheckpoint)
        ).toVector.sortBy(_._1).map(_._2)

        return ExecutionResult(
          wasUpToDate = false,
          checkpoint = MultiSourceDownloadCheckpoint(
            lastDownloaded = result.checkpoint.lastDownloaded,
            eventTime = result.checkpoint.eventTime,
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
