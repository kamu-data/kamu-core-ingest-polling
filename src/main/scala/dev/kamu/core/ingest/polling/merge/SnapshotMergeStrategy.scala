/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.core.ingest.polling.merge
import java.sql.Timestamp

import dev.kamu.core.ingest.polling.utils.DFUtils._
import dev.kamu.core.ingest.polling.utils.TimeSeriesUtils
import dev.kamu.core.manifests.DatasetVocabulary
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit, when}

/** Snapshot data merge strategy.
  *
  * This strategy should be used for data dumps that are taken periodically
  * and only contain only the latest state of the observed entity or system.
  * Over time such dumps can have new rows added, and old rows either missing
  * or changed.
  *
  * This strategy transforms snapshot data into an append-only event stream
  * where data already added is immutable. It does so by treating rows in
  * snapshots as "observation" events and adding an "observed" column
  * that will contain:
  *   - "I" - when a row appears for the first time
  *   - "D" - when row disappears
  *   - "U" - whenever any row data has changed
  *
  * This strategy relies on a user-specified primary key column to
  * correlate the rows between two snapshots.
  *
  * To determine if the row has changed between two snapshots all columns are
  * compared one by one. Optionally the set of columns to compare can be
  * specified explicitly. If the data contains a column that is guaranteed to
  * change whenever any of the data columns changes (for example a modification
  * timestamp, an incremental version, or a data hash) - using it as a
  * modification indicator will significantly speed up the detection of
  * modified rows.
  *
  * This strategy will always preserve all columns from existing and
  * new snapshots, so the set of columns can only grow.
  *
  * @param primaryKey     primary key column names
  * @param compareColumns columns to compare to determine if a row has changed
  * @param vocab          vocabulary of system column names and values
  */
class SnapshotMergeStrategy(
  primaryKey: Vector[String],
  compareColumns: Vector[String] = Vector.empty,
  vocab: DatasetVocabulary = DatasetVocabulary()
) extends MergeStrategy {

  /** Performs diff-based merge.
    *
    * Equivalent SQL:
    * {{{
    * SELECT
    *   ${systemTime} as systemTime,
    *   CASE
    *     WHEN prev.${pk} IS NULL THEN "I"
    *     WHEN curr.${pk} IS NULL THEN "D"
    *     ELSE "U"
    *   END AS observed,
    *   CASE
    *     WHEN curr.${pk} IS NULL THEN prev.A
    *     ELSE curr.A
    *   END AS A,
    *   ...
    *   CASE
    *     WHEN curr.${pk} IS NULL THEN prev.Z
    *     ELSE curr.A
    *   END AS Z,
    *   COALESCE(curr.A, prev.A) as A
    * FROM curr
    * FULL OUTER JOIN prev
    *   ON curr.%{pk} = prev.%{pk}
    * WHERE curr.${pk} IS NULL
    *   OR prev.${pk} IS NULL
    *   OR curr.${modInd} != prev.${modInd}
    * }}}
    */
  override def merge(
    prevSeries: Option[DataFrame],
    curr: DataFrame,
    systemTime: Timestamp
  ): DataFrame = {
    val prev = if (prevSeries.isDefined) {
      TimeSeriesUtils
        .asOf(prevSeries.get, primaryKey)
        .drop(vocab.lastUpdatedTimeSystemColumn)
    } else {
      TimeSeriesUtils.empty(curr)
    }

    val combinedDataColumnNames = (prev.columns ++ curr.columns).distinct.toList

    val columnsToCompare =
      if (compareColumns.nonEmpty) compareColumns else combinedDataColumnNames

    // We consider data changed when
    // either both columns exist and have different values
    // or column disappears while having non-null value
    // or column is added with non-null value
    val changedPredicate = columnsToCompare
      .map(columnName => {
        val pc = prev.getColumn(columnName)
        val cc = curr.getColumn(columnName)
        if (pc.isDefined && cc.isDefined) {
          pc.get =!= cc.get
        } else if (pc.isDefined) {
          pc.get.isNotNull
        } else if (cc.isDefined) {
          cc.get.isNotNull
        } else {
          throw new RuntimeException(s"Column does not exist: $columnName")
        }
      })
      .foldLeft(lit(false))((a, b) => a || b)

    def columnOrNull(df: DataFrame, name: String) =
      df.getColumn(name).getOrElse(lit(null))

    val resultDataColumns = combinedDataColumnNames
      .map(
        columnName =>
          when(
            col(vocab.observationColumn) === vocab.obsvRemoved,
            columnOrNull(prev, columnName)
          ).otherwise(columnOrNull(curr, columnName))
            .as(columnName)
      )

    val resultColumns = col(vocab.systemTimeColumn) :: col(
      vocab.observationColumn
    ) :: resultDataColumns

    curr
      .join(
        prev,
        primaryKey.map(c => prev(c) <=> curr(c)).reduce(_ && _),
        "full_outer"
      )
      .filter(
        primaryKey.map(curr(_).isNull).reduce(_ && _) ||
          primaryKey.map(prev(_).isNull).reduce(_ && _) ||
          changedPredicate
      )
      .withColumn(vocab.systemTimeColumn, lit(systemTime))
      .withColumn(
        vocab.observationColumn,
        when(
          primaryKey.map(prev(_).isNull).reduce(_ && _),
          vocab.obsvAdded
        ).when(
            primaryKey.map(curr(_).isNull).reduce(_ && _),
            vocab.obsvRemoved
          )
          .otherwise(vocab.obsvChanged)
      )
      .select(resultColumns: _*)
  }
}
