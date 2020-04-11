/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.core.ingest.polling.poll

import dev.kamu.core.manifests.{CachingKind, EventTimeKind, FetchKind}
import dev.kamu.core.utils.Clock
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.LogManager

class SourceFactory(fileSystem: FileSystem, systemClock: Clock) {
  private val logger = LogManager.getLogger(getClass.getName)

  def getSource(kind: FetchKind): Seq[CacheableSource] = {
    val eventTimeSource = kind.eventTime match {
      case None | Some(_: EventTimeKind.FromSystemTime) =>
        new EventTimeSource.FromSystemTime(systemClock)
      case Some(e: EventTimeKind.FromPath) =>
        new EventTimeSource.FromPath(e.pattern, e.timestampFormat)
      case _ =>
        throw new NotImplementedError(
          s"Unsupported event time source: ${kind.eventTime}"
        )
    }

    kind match {
      case fetch: FetchKind.FetchUrl =>
        getFetchUrlSource(fetch, eventTimeSource)
      case glob: FetchKind.FetchFilesGlob =>
        getFetchFilesGlobSource(glob, eventTimeSource)
    }
  }

  def getFetchUrlSource(
    kind: FetchKind.FetchUrl,
    eventTimeSource: EventTimeSource
  ): Seq[CacheableSource] = {
    kind.url.getScheme match {
      case "http" | "https" =>
        Seq(
          new HTTPSource(
            "primary",
            systemClock,
            kind.url,
            eventTimeSource
          )
        )
      case "ftp" =>
        Seq(
          new FTPSource(
            "primary",
            systemClock,
            kind.url,
            eventTimeSource
          )
        )
      case "gs" | "hdfs" | "file" | null =>
        // TODO: restrict allowed source paths for security
        Seq(
          new FileSystemSource(
            "primary",
            fileSystem,
            systemClock,
            new Path(kind.url),
            eventTimeSource
          )
        )
      case _ =>
        throw new NotImplementedError(s"Unsupported source: ${kind.url}")
    }
  }

  def getFetchFilesGlobSource(
    kind: FetchKind.FetchFilesGlob,
    eventTimeSource: EventTimeSource
  ): Seq[CacheableSource] = {
    val globbed = fileSystem
      .globStatus(kind.path)
      .map(
        f =>
          new FileSystemSource(
            f.getPath.getName,
            fileSystem,
            systemClock,
            f.getPath,
            eventTimeSource
          )
      )
      .sortBy(eventTimeSource.getEventTime)

    logger.info(
      s"Glob pattern resolved to: ${globbed.map(_.path.getName).mkString(", ")}"
    )

    globbed
  }

  def getCachingBehavior(kind: FetchKind): CachingBehavior = {
    val cacheSettings = kind match {
      case url: FetchKind.FetchUrl        => url.cache
      case glob: FetchKind.FetchFilesGlob => glob.cache
    }
    cacheSettings match {
      case None                         => new CachingBehaviorDefault()
      case Some(_: CachingKind.Forever) => new CachingBehaviorForever()
    }
  }
}
