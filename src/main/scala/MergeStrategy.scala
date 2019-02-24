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
        new LedgerMergeStrategy()
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
  */
class LedgerMergeStrategy (
) extends MergeStrategy {

  override def merge(prev: Option[DataFrame], curr: DataFrame, systemTime: Timestamp): DataFrame = {
    throw new NotImplementedError()
  }

}


/** Snapshot data merge strategy.
  *
  * This strategy should be used for data dumps that are taken periodically
  * and only contain latest state. New data can have new rows added, and old
  * rows either missing or changed.
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
  * For faster detection of modified rows this strategy also depends on
  * user-specified modification indicator column that is guaranteed to
  * change when any of the row data changes. This can be a column that
  * contains a las modification date, an incremental version, or a data hash.
  *
  * This strategy will always preserve all columns from existing and
  * new snapshots, so the set of columns can only grow.
  *
  * @param pk primary key column name
  * @param modInd modification indicator column name
  * @param vocab vocabulary of system column names and values
  */
class SnapshotMergeStrategy(
  pk: String,
  modInd: String,
  vocab: Vocabulary = Vocabulary()
) extends MergeStrategy {
  val systemTimeColumn = "systemTime"
  val observedColumn = "observed"
  val obsvAdded = "added"
  val obsvRemoved = "removed"
  val obsvUpdated = "updated"

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

    val dataColumns = (prev.columns ++ curr.columns).distinct.toList
      .map(columnName =>
        when(
          col(observedColumn) === obsvRemoved,
          prev.getColumn(columnName).getOrElse(lit(null)))
          .otherwise(
            curr.getColumn(columnName).getOrElse(lit(null)))
          .as(columnName))

    val allColumns = col(systemTimeColumn) :: col(observedColumn) :: dataColumns

    curr
      .join(prev, prev(pk) === curr(pk), "full_outer")
      .filter(
        curr(pk).isNull || prev(pk).isNull || curr(modInd) =!= prev(modInd))
      .withColumn(
        systemTimeColumn,
        lit(systemTime))
      .withColumn(
        observedColumn,
        when(prev(pk).isNull, obsvAdded)
          .when(curr(pk).isNull, obsvRemoved)
          .otherwise(obsvUpdated))
      .select(allColumns: _*)
  }
}
