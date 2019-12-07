/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.core.ingest.polling.merge

import java.sql.Timestamp

import dev.kamu.core.ingest.polling.utils.TimeSeriesUtils
import dev.kamu.core.ingest.polling.utils.DFUtils._
import dev.kamu.core.manifests
import dev.kamu.core.manifests.DatasetVocabulary
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit

object MergeStrategy {
  def apply(kind: manifests.MergeStrategyKind): MergeStrategy = {
    kind match {
      case _: manifests.MergeStrategyKind.Append =>
        new AppendMergeStrategy()
      case c: manifests.MergeStrategyKind.Ledger =>
        new LedgerMergeStrategy(c.primaryKey)
      case c: manifests.MergeStrategyKind.Snapshot =>
        new SnapshotMergeStrategy(
          primaryKey = c.primaryKey,
          compareColumns = c.compareColumns
        )
      case _ =>
        throw new NotImplementedError(s"Unsupported strategy: $kind")
    }
  }
}

abstract class MergeStrategy(vocab: DatasetVocabulary) {

  /** Performs merge-in of the data.
    *
    * @param prev data that is already stored in the system (if any)
    * @param curr data to be added
    * @param systemTime system wall clock to be added to all records
    * @param eventTime event time to add to new rows if not already contained in the data
    */
  def merge(
    prev: Option[DataFrame],
    curr: DataFrame,
    systemTime: Timestamp,
    eventTime: Option[Timestamp]
  ): DataFrame

  protected def prepare(
    prevRaw: Option[DataFrame],
    currRaw: DataFrame,
    systemTime: Timestamp,
    eventTime: Option[Timestamp]
  ): (DataFrame, DataFrame, Set[String], Set[String]) = {
    if (currRaw.getColumn(vocab.systemTimeColumn).isDefined)
      throw new Exception(
        s"Data already contains column: ${vocab.systemTimeColumn}"
      )

    if (eventTime.isEmpty && !currRaw.hasColumn(vocab.eventTimeColumn))
      throw new Exception(
        s"Event time column is neither specified in bulk nor exists in raw data: ${vocab.eventTimeColumn}"
      )

    val currWithTimes = currRaw
      .withColumn(vocab.systemTimeColumn, lit(systemTime))
      .maybeTransform(
        !currRaw.hasColumn(vocab.eventTimeColumn),
        _.withColumn(vocab.eventTimeColumn, lit(eventTime.get))
      )

    val prevOrEmpty = prevRaw.getOrElse(TimeSeriesUtils.empty(currWithTimes))

    val combinedColumns =
      (prevOrEmpty.columns ++ currWithTimes.columns).distinct

    val addedColumns =
      combinedColumns.filter(!prevOrEmpty.hasColumn(_)).toSet
    val removedColumns =
      combinedColumns.filter(!currWithTimes.hasColumn(_)).toSet

    val prev = prevOrEmpty.select(
      combinedColumns.map(
        c =>
          prevOrEmpty
            .getColumn(c)
            .getOrElse(lit(null))
            .as(c)
      ): _*
    )

    val curr = currWithTimes.select(
      combinedColumns.map(
        c =>
          currWithTimes
            .getColumn(c)
            .getOrElse(lit(null))
            .as(c)
      ): _*
    )

    (
      prev,
      curr,
      addedColumns,
      removedColumns
    )
  }

  protected def orderColumns(dataFrame: DataFrame): DataFrame = {
    val columns = Seq(
      vocab.systemTimeColumn,
      vocab.eventTimeColumn,
      vocab.observationColumn
    ).filter(dataFrame.getColumn(_).isDefined)
    dataFrame.columnToFront(columns: _*)
  }
}
