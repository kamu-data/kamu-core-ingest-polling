/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.core.ingest.polling.poll

import org.apache.log4j.LogManager

trait CachingBehavior {
  protected val logger = LogManager.getLogger(getClass.getName)

  def shouldDownload(storedCheckpoint: Option[DownloadCheckpoint]): Boolean
}

class CachingBehaviorDefault extends CachingBehavior {
  override def shouldDownload(
    storedCheckpoint: Option[DownloadCheckpoint]
  ): Boolean = {
    if (storedCheckpoint.isDefined) {
      if (!storedCheckpoint.get.isCacheable) {
        logger.warn("Skipping uncachable source")
        return false
      }
    }
    true
  }
}

class CachingBehaviorForever extends CachingBehavior {
  override def shouldDownload(
    storedCheckpoint: Option[DownloadCheckpoint]
  ): Boolean = {
    if (!storedCheckpoint.isDefined)
      return true

    logger.warn("Skipping source as it's cached forever")
    false
  }
}
