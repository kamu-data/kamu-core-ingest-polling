/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.core.ingest.polling.merge

import java.sql.Timestamp

import dev.kamu.core.manifests.DatasetVocabulary
import org.apache.spark.sql.DataFrame

/** Append merge strategy.
  *
  * Under this strategy polled data will be appended in its original form
  * to the already ingested data. Optionally can add the system time column.
  *
  * @param vocab vocabulary of system column names and common values
  */
class AppendMergeStrategy(
  vocab: DatasetVocabulary = DatasetVocabulary()
) extends MergeStrategy(vocab) {

  override def merge(
    prevRaw: Option[DataFrame],
    currRaw: DataFrame,
    systemTime: Timestamp,
    eventTime: Option[Timestamp]
  ): DataFrame = {
    val (_, curr, _, _) = prepare(prevRaw, currRaw, systemTime, eventTime)

    orderColumns(curr)
  }

}
