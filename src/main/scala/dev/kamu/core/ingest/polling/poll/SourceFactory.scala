/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.core.ingest.polling.poll

import dev.kamu.core.manifests.{ExternalSourceFetchUrl, ExternalSourceKind}
import org.apache.hadoop.fs.FileSystem
import org.apache.log4j.LogManager

class SourceFactory(fileSystem: FileSystem) {
  private val logger = LogManager.getLogger(getClass.getName)

  def getSource(kind: ExternalSourceKind): CacheableSource = {
    kind match {
      case fetch: ExternalSourceFetchUrl =>
        fetch.url.getScheme match {
          case "http" | "https" =>
            new HTTPSource(fetch.url)
          case "ftp" =>
            new FTPSource(fetch.url)
          case "gs" =>
            new FileSystemSource(fileSystem, fetch.url)
          case "hdfs" | "file" | null =>
            // TODO: restrict allowed source paths for security
            new FileSystemSource(fileSystem, fetch.url)
          case _ =>
            throw new NotImplementedError(s"Unsupported source: ${fetch.url}")
        }
    }
  }
}
