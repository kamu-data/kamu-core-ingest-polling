/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.core.ingest.polling.poll

import java.io.InputStream

import dev.kamu.core.ingest.polling.utils.ExecutionResult
import org.apache.log4j.LogManager

trait CacheableSource {
  protected val logger = LogManager.getLogger(getClass.getName)

  def sourceID: String

  def maybeDownload(
    cacheInfo: Option[DownloadCheckpoint],
    handler: InputStream => Unit
  ): ExecutionResult[DownloadCheckpoint]
}
