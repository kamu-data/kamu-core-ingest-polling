/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.core.ingest.polling.poll

import java.sql.Timestamp

import dev.kamu.core.ingest.polling.poll
import dev.kamu.core.manifests.{CachingKind, EventTimeKind, ExternalSourceKind}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.LogManager

class SourceFactory(fileSystem: FileSystem, getSystemTime: () => Timestamp) {
  private val logger = LogManager.getLogger(getClass.getName)

  def getSource(kind: ExternalSourceKind): CacheableSource = {
    val eventTimeSource = kind.eventTime match {
      case None =>
        new poll.EventTimeSource.NoEventTime()
      case Some(_: EventTimeKind.FromSystemTime) =>
        new EventTimeSource.FromSystemTime(getSystemTime)
      case Some(e: EventTimeKind.FromPath) =>
        new EventTimeSource.FromPath(e.pattern, e.timestampFormat)
      case _ =>
        throw new NotImplementedError(
          s"Unsupported event time source: ${kind.eventTime}"
        )
    }

    kind match {
      case fetch: ExternalSourceKind.FetchUrl =>
        fetch.url.getScheme match {
          case "http" | "https" =>
            new HTTPSource(
              fetch.url,
              eventTimeSource
            )
          case "ftp" =>
            new FTPSource(
              fetch.url,
              eventTimeSource
            )
          case "gs" =>
            new FileSystemSource(
              fileSystem,
              new Path(fetch.url),
              eventTimeSource
            )
          case "hdfs" | "file" | null =>
            // TODO: restrict allowed source paths for security
            new FileSystemSource(
              fileSystem,
              new Path(fetch.url),
              eventTimeSource
            )
          case _ =>
            throw new NotImplementedError(s"Unsupported source: ${fetch.url}")
        }
      case glob: ExternalSourceKind.FetchFilesGlob =>
        new FileSystemGlobSource(fileSystem, glob.path, eventTimeSource)
    }
  }

  def getCachingBehavior(kind: ExternalSourceKind): CachingBehavior = {
    val cacheSettings = kind match {
      case url: ExternalSourceKind.FetchUrl        => url.cache
      case glob: ExternalSourceKind.FetchFilesGlob => glob.cache
    }
    cacheSettings match {
      case None                         => new CachingBehaviorDefault()
      case Some(_: CachingKind.Forever) => new CachingBehaviorForever()
    }
  }
}
