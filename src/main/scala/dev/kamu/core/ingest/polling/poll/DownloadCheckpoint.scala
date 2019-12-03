/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.core.ingest.polling.poll

import java.time.Instant

import dev.kamu.core.manifests.Resource

sealed trait DownloadCheckpoint extends Resource[DownloadCheckpoint] {
  def lastDownloaded: Instant
  def eventTime: Option[Instant]
  def isCacheable: Boolean
}

case class SimpleDownloadCheckpoint(
  lastDownloaded: Instant,
  eventTime: Option[Instant],
  lastModified: Option[Instant] = None,
  eTag: Option[String] = None
) extends DownloadCheckpoint {
  override def isCacheable: Boolean = lastModified.isDefined || eTag.isDefined
}

case class MultiSourceDownloadCheckpoint(
  lastDownloaded: Instant,
  eventTime: Option[Instant],
  children: Vector[MultiSourceDownloadCheckpoint.ChildCheckpoint] = Vector.empty
) extends DownloadCheckpoint {
  override def isCacheable: Boolean = true
}

object MultiSourceDownloadCheckpoint {
  case class ChildCheckpoint(
    source: String,
    checkpoint: SimpleDownloadCheckpoint
  )
}
