/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.core.ingest.polling.utils

import java.io.{FilterInputStream, InputStream}
import java.util.zip.{ZipEntry, ZipInputStream}

object ZipEntryStream {
  def first(inputStream: InputStream): ZipEntryStream = {
    val zipStream = new ZipInputStream(inputStream)
    new ZipEntryStream(zipStream, zipStream.getNextEntry)
  }

  def findFirst(
    inputStream: InputStream,
    regex: String
  ): Option[ZipEntryStream] = {
    val zipStream = new ZipInputStream(inputStream)
    var entry = zipStream.getNextEntry
    while (entry != null) {
      if (entry.getName.matches(regex))
        return Some(new ZipEntryStream(zipStream, entry))
      entry = zipStream.getNextEntry
    }
    None
  }
}

class ZipEntryStream private (
  val zipStream: ZipInputStream,
  val entry: ZipEntry
) extends FilterInputStream(zipStream)
