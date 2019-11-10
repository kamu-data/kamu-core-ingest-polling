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
  * If the data contains a column that is guaranteed to change whenever
  * any of the data columns changes (for example this can be a last
  * modification timestamp, an incremental version, or a data hash), then
  * it can be specified as modification indicator to speed up the detection of
  * modified rows.
  *
  * This strategy will always preserve all columns from existing and
  * new snapshots, so the set of columns can only grow.
  *
  * @param pk     primary key column name
  * @param modInd column that always changes when the rest of the row is modified
  * @param vocab  vocabulary of system column names and values
  */
class SnapshotMergeStrategy(
  pk: Vector[String],
  modInd: Option[String],
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
        .asOf(prevSeries.get, pk)
        .drop(vocab.lastUpdatedTimeSystemColumn)
    } else {
      TimeSeriesUtils.empty(curr)
    }

    val combinedDataColumnNames = (prev.columns ++ curr.columns).distinct.toList

    val changedPredicate = if (modInd.isDefined) {
      curr(modInd.get) =!= prev(modInd.get)
    } else {
      // We consider data changed when
      // either both columns exist and have different values
      // or column disappears while having non-null value
      // or column is added with non-null value
      combinedDataColumnNames
        .map(columnName => {
          val pc = prev.getColumn(columnName)
          val cc = curr.getColumn(columnName)
          if (pc.isDefined && cc.isDefined) {
            pc.get =!= cc.get
          } else if (pc.isDefined) {
            pc.get.isNotNull
          } else {
            cc.get.isNotNull
          }
        })
        .foldLeft(lit(false))((a, b) => a || b)
    }

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
      .join(prev, pk.map(c => prev(c) <=> curr(c)).reduce(_ && _), "full_outer")
      .filter(
        pk.map(curr(_).isNull).reduce(_ && _) ||
          pk.map(prev(_).isNull).reduce(_ && _) ||
          changedPredicate
      )
      .withColumn(vocab.systemTimeColumn, lit(systemTime))
      .withColumn(
        vocab.observationColumn,
        when(pk.map(prev(_).isNull).reduce(_ && _), vocab.obsvAdded)
          .when(pk.map(curr(_).isNull).reduce(_ && _), vocab.obsvRemoved)
          .otherwise(vocab.obsvChanged)
      )
      .select(resultColumns: _*)
  }
}
