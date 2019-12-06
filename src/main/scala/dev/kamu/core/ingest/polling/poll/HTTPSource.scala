/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.core.ingest.polling.poll

import java.io.InputStream
import java.net.URI
import java.text.SimpleDateFormat
import java.time.format.DateTimeFormatter
import java.time.{Instant, ZonedDateTime}

import dev.kamu.core.ingest.polling.utils.ExecutionResult
import scalaj.http.Http

class HTTPSource(
  val sourceID: String,
  url: URI,
  eventTimeSource: EventTimeSource
) extends CacheableSource {

  private val lastModifiedHeaderFormat = new SimpleDateFormat(
    "EEE, dd MMM yyyy HH:mm:ss zzz"
  )

  override def maybeDownload(
    checkpoint: Option[DownloadCheckpoint],
    cachingBehavior: CachingBehavior,
    handler: InputStream => Unit
  ): ExecutionResult[DownloadCheckpoint] = {
    if (!cachingBehavior.shouldDownload(checkpoint))
      return ExecutionResult(
        wasUpToDate = true,
        checkpoint = checkpoint.get
      )

    var request = Http(url.toString)
      .timeout(connTimeoutMs = 30 * 1000, readTimeoutMs = 30 * 1000)
      .method("GET")

    if (checkpoint.isDefined) {
      val ci = checkpoint.get

      if (ci.eTag.isDefined)
        request = request
          .header("If-None-Match", ci.eTag.get)

      if (ci.lastModified.isDefined)
        request = request
          .header(
            "If-Modified-Since",
            lastModifiedHeaderFormat.format(ci.lastModified)
          )
    }

    logger.info(s"HTTP GET $url")

    // TODO: this will write body even in case of error
    val response = request.exec((code, _, bodyStream) => {
      if (code == 200)
        handler(bodyStream)
    })

    response.code match {
      case 200 =>
        ExecutionResult(
          wasUpToDate = false,
          checkpoint = DownloadCheckpoint(
            lastModified = response
              .header("LastModified")
              .map(
                s =>
                  ZonedDateTime
                    .parse(s, DateTimeFormatter.RFC_1123_DATE_TIME)
                    .toInstant
              ),
            eTag = response.header("ETag"),
            lastDownloaded = Instant.now(),
            eventTime = eventTimeSource.getEventTime(this)
          )
        )
      case 304 =>
        ExecutionResult(wasUpToDate = true, checkpoint.get)
      case _ =>
        throw new RuntimeException(s"Request failed: ${response.statusLine}")
    }
  }
}
