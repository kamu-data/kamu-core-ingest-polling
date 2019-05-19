import java.sql.Timestamp

import DFUtils._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit, when}


object MergeStrategy {
  def apply(conf: MergeStrategyConf): MergeStrategy = {
    conf match {
      case c: Append =>
        new AppendMergeStrategy(
          addSystemTime = c.addSystemTime)
      case c: Ledger =>
        new LedgerMergeStrategy(c.primaryKey)
      case c: Snapshot =>
        new SnapshotMergeStrategy(
          pk = c.primaryKey,
          modInd = c.modificationIndicator)
      case _ =>
        throw new NotImplementedError(s"Unsupported strategy: $conf")
    }
  }
}


abstract class MergeStrategy {
  def merge(prev: Option[DataFrame], curr: DataFrame, systemTime: Timestamp): DataFrame
}


/** Append merge strategy.
  *
  * Under this strategy polled data will be appended in its original form
  * to the already ingested data. Optionally can add the system time column.
  *
  * @param addSystemTime whether to add system time column to data
  * @param vocab vocabulary of system column names and common values
  */
class AppendMergeStrategy (
  addSystemTime: Boolean = false,
  vocab: Vocabulary = Vocabulary()
) extends MergeStrategy {

  override def merge(prev: Option[DataFrame], curr: DataFrame, systemTime: Timestamp): DataFrame = {
    if (addSystemTime) {
      curr.withColumn(vocab.systemTimeColumn, lit(systemTime))
    } else {
      curr
    }
  }

}


/** Ledger merge strategy.
  *
  * This strategy should be used for data dumps containing append-only event
  * streams. New data dumps can have new rows added, but once data already
  * made it into one dump it never changes or disappears.
  *
  * A system time column will be added to the data to indicate the time
  * when the record was observed first by the system.
  *
  * It relies on a user-specified primary key column to identify which records
  * were already seen and not duplicate them.
  *
  * It will always preserve all columns from existing and new snapshots, so
  * the set of columns can only grow.
  *
  * @param pk primary key column name
  */
class LedgerMergeStrategy (
  pk: String,
  vocab: Vocabulary = Vocabulary()
) extends MergeStrategy {

  override def merge(
    prevSeries: Option[DataFrame], currSeries: DataFrame,
    systemTime: Timestamp
  ): DataFrame = {
    val curr = currSeries
      .withColumn(vocab.systemTimeColumn, lit(systemTime))
      .columnToFront(vocab.systemTimeColumn)

    val prev = prevSeries.getOrElse(TimeSeriesUtils.empty(curr))

    val combinedColumnNames = (prev.columns ++ curr.columns).distinct.toList

    val resultColumns = combinedColumnNames
      .map(columnName =>
        curr.getColumn(columnName)
          .getOrElse(lit(null))
          .as(columnName))

    curr
      .join(prev, curr(pk) === prev(pk), "left_outer")
      .filter(prev(pk).isNull)
      .select(resultColumns: _*)
  }

}


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
  *   - "added" - when a row appears for the first time
  *   - "removed" - when row disappears
  *   - "updated" - whenever any row data has changed
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
  * @param pk primary key column name
  * @param modInd optional modification indicator column name
  * @param vocab vocabulary of system column names and values
  */
class SnapshotMergeStrategy(
  pk: String,
  modInd: Option[String],
  vocab: Vocabulary = Vocabulary()
) extends MergeStrategy {

  /** Performs snapshot merge.
    *
    * Equivalent SQL:
    * {{{
    * SELECT
    *   ${systemTime} as systemTime,
    *   CASE
    *     WHEN prev.${pk} IS NULL THEN "added"
    *     WHEN curr.${pk} IS NULL THEN "removed"
    *     ELSE "changed"
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
  override def merge(prevSeries: Option[DataFrame], curr: DataFrame, systemTime: Timestamp): DataFrame = {
    val prev = if (prevSeries.isDefined) {
      TimeSeriesUtils
        .timeSeriesToSnapshot(prevSeries.get, pk)
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
      .map(columnName =>
        when(
          col(vocab.observationColumn) === vocab.obsvRemoved,
          columnOrNull(prev, columnName))
        .otherwise(columnOrNull(curr, columnName))
        .as(columnName))

    val resultColumns = col(vocab.systemTimeColumn) :: col(vocab.observationColumn) :: resultDataColumns

    curr
      .join(prev, prev(pk) === curr(pk), "full_outer")
      .filter(
        curr(pk).isNull || prev(pk).isNull || changedPredicate)
      .withColumn(
        vocab.systemTimeColumn,
        lit(systemTime))
      .withColumn(
        vocab.observationColumn,
        when(prev(pk).isNull, vocab.obsvAdded)
          .when(curr(pk).isNull, vocab.obsvRemoved)
          .otherwise(vocab.obsvChanged))
      .select(resultColumns: _*)
  }
}