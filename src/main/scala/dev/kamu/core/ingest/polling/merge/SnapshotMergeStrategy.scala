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
import org.apache.spark.sql.functions.{col, lit, when, min, max}

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
) extends MergeStrategy(vocab) {

  override def merge(
    prevRaw: Option[DataFrame],
    currRaw: DataFrame,
    systemTime: Timestamp,
    eventTime: Option[Timestamp]
  ): DataFrame = {
    val (prev, curr, addedColumns, removedColumns) =
      prepare(prevRaw, currRaw, systemTime, eventTime)

    ensureEventTimeDoesntGoBackwards(prev, curr)

    val dataColumns = curr.columns
      .filter(
        c =>
          c != vocab.systemTimeColumn && c != vocab.eventTimeColumn && c != vocab.observationColumn
      )
      .toVector

    val columnsToCompare =
      if (compareColumns.nonEmpty) {
        val invalidColumns = compareColumns.filter(!curr.hasColumn(_))
        if (invalidColumns.nonEmpty)
          throw new RuntimeException(
            s"Column does not exist: " + invalidColumns.mkString(", ")
          )
        compareColumns
      } else {
        dataColumns.filter(!primaryKey.contains(_))
      }

    val prevProj = TimeSeriesUtils
      .asOf(prev, primaryKey, vocab.eventTimeColumn, None, vocab)

    // We consider data changed when
    // either both columns exist and have different values
    // or column disappears while having non-null value
    // or column is added with non-null value
    val changedPredicate = columnsToCompare
      .map(c => {
        if (addedColumns.contains(c)) {
          curr(c).isNotNull
        } else if (removedColumns.contains(c)) {
          prevProj(c).isNotNull
        } else {
          prevProj(c) =!= curr(c)
        }
      })
      .foldLeft(lit(false))((a, b) => a || b)

    val resultDataColumns = dataColumns
      .map(
        c =>
          when(
            col(vocab.observationColumn) === vocab.obsvRemoved,
            prevProj(c)
          ).otherwise(curr(c))
            .as(c)
      )

    val resultColumns = (
      lit(systemTime).as(vocab.systemTimeColumn)
        :: when(
          col(vocab.observationColumn) === vocab.obsvRemoved,
          lit(eventTime.getOrElse(systemTime))
        ).otherwise(curr(vocab.eventTimeColumn)).as(vocab.eventTimeColumn)
        :: col(vocab.observationColumn)
        :: resultDataColumns.toList
    )

    val result = curr
      .drop(vocab.systemTimeColumn, vocab.observationColumn)
      .join(
        prevProj,
        primaryKey.map(c => prevProj(c) <=> curr(c)).reduce(_ && _),
        "full_outer"
      )
      .filter(
        primaryKey.map(curr(_).isNull).reduce(_ && _)
          || primaryKey.map(prevProj(_).isNull).reduce(_ && _)
          || changedPredicate
      )
      .withColumn(
        vocab.observationColumn,
        when(
          primaryKey.map(prevProj(_).isNull).reduce(_ && _),
          vocab.obsvAdded
        ).when(
            primaryKey.map(curr(_).isNull).reduce(_ && _),
            vocab.obsvRemoved
          )
          .otherwise(vocab.obsvChanged)
      )
      .dropDuplicates(primaryKey)
      .select(resultColumns: _*)

    orderColumns(result)
  }

  def ensureEventTimeDoesntGoBackwards(
    prev: DataFrame,
    curr: DataFrame
  ): Unit = {
    val lastSeenMaxEventTime =
      prev.agg(max(vocab.eventTimeColumn)).head().getTimestamp(0)
    val newMinEventTime =
      curr.agg(min(vocab.eventTimeColumn)).head().getTimestamp(0)

    if (lastSeenMaxEventTime != null && newMinEventTime != null && lastSeenMaxEventTime
          .compareTo(newMinEventTime) >= 0) {
      throw new Exception(
        s"Past event time was seen, snapshots don't support adding data out of order: $lastSeenMaxEventTime >= $newMinEventTime"
      )
    }
  }

  override protected def prepare(
    prevRaw: Option[DataFrame],
    currRaw: DataFrame,
    systemTime: Timestamp,
    eventTime: Option[Timestamp]
  ): (DataFrame, DataFrame, Set[String], Set[String]) = {
    if (currRaw.hasColumn(vocab.observationColumn))
      throw new Exception(
        s"Data already contains column: ${vocab.observationColumn}"
      )

    val (prev, curr, addedColumns, removedColumns) =
      super.prepare(prevRaw, currRaw, systemTime, eventTime)

    (
      prev.maybeTransform(
        !prev.hasColumn(vocab.observationColumn),
        _.withColumn(vocab.observationColumn, lit(vocab.obsvAdded))
      ),
      curr
        .maybeTransform(
          !curr.hasColumn(vocab.observationColumn),
          _.withColumn(vocab.observationColumn, lit(vocab.obsvAdded))
        ),
      addedColumns,
      removedColumns - vocab.observationColumn
    )
  }
}
