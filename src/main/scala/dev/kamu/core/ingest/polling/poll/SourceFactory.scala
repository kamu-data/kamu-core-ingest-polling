/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.core.ingest.polling.poll

import java.sql.Timestamp

import dev.kamu.core.manifests.{CachingKind, EventTimeKind, ExternalSourceKind}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.LogManager

class SourceFactory(fileSystem: FileSystem, getSystemTime: () => Timestamp) {
  private val logger = LogManager.getLogger(getClass.getName)

  def getSource(kind: ExternalSourceKind): Seq[CacheableSource] = {
    val eventTimeSource = kind.eventTime match {
      case None | Some(_: EventTimeKind.FromSystemTime) =>
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
        getFetchUrlSource(fetch, eventTimeSource)
      case glob: ExternalSourceKind.FetchFilesGlob =>
        getFetchFilesGlobSource(glob, eventTimeSource)
    }
  }

  def getFetchUrlSource(
    kind: ExternalSourceKind.FetchUrl,
    eventTimeSource: EventTimeSource
  ): Seq[CacheableSource] = {
    kind.url.getScheme match {
      case "http" | "https" =>
        Seq(
          new HTTPSource(
            "primary",
            kind.url,
            eventTimeSource
          )
        )
      case "ftp" =>
        Seq(
          new FTPSource(
            "primary",
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
            new Path(kind.url),
            eventTimeSource
          )
        )
      case _ =>
        throw new NotImplementedError(s"Unsupported source: ${kind.url}")
    }
  }

  def getFetchFilesGlobSource(
    kind: ExternalSourceKind.FetchFilesGlob,
    eventTimeSource: EventTimeSource
  ): Seq[CacheableSource] = {
    val globbed = fileSystem
      .globStatus(kind.path)
      .map(
        f =>
          new FileSystemSource(
            f.getPath.getName,
            fileSystem,
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
