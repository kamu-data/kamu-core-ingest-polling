/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.core.ingest.polling.merge

import java.sql.Timestamp

import dev.kamu.core.manifests
import org.apache.spark.sql.DataFrame

object MergeStrategy {
  def apply(kind: manifests.MergeStrategyKind): MergeStrategy = {
    kind match {
      case _: manifests.MergeStrategyAppend =>
        new AppendMergeStrategy()
      case c: manifests.MergeStrategyLedger =>
        new LedgerMergeStrategy(c.primaryKey)
      case c: manifests.MergeStrategySnapshot =>
        new SnapshotMergeStrategy(
          primaryKey = c.primaryKey,
          compareColumns = c.compareColumns
        )
      case _ =>
        throw new NotImplementedError(s"Unsupported strategy: $kind")
    }
  }
}

abstract class MergeStrategy {
  def merge(
    prev: Option[DataFrame],
    curr: DataFrame,
    systemTime: Timestamp
  ): DataFrame
}
