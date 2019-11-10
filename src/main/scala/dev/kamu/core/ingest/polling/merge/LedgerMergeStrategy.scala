package dev.kamu.core.ingest.polling.merge

import java.sql.Timestamp

import dev.kamu.core.ingest.polling.utils.DFUtils._
import dev.kamu.core.ingest.polling.utils.TimeSeriesUtils
import dev.kamu.core.manifests.DatasetVocabulary
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit

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
class LedgerMergeStrategy(
  pk: Vector[String],
  vocab: DatasetVocabulary = DatasetVocabulary()
) extends MergeStrategy {

  override def merge(
    prevSeries: Option[DataFrame],
    currSeries: DataFrame,
    systemTime: Timestamp
  ): DataFrame = {
    val curr = currSeries
      .withColumn(vocab.systemTimeColumn, lit(systemTime))
      .columnToFront(vocab.systemTimeColumn)

    val prev = prevSeries.getOrElse(TimeSeriesUtils.empty(curr))

    val combinedColumnNames = (prev.columns ++ curr.columns).distinct.toList

    val resultColumns = combinedColumnNames
      .map(
        columnName =>
          curr
            .getColumn(columnName)
            .getOrElse(lit(null))
            .as(columnName)
      )

    curr
      .join(prev, pk.map(c => curr(c) <=> prev(c)).reduce(_ && _), "left_outer")
      .filter(pk.map(c => prev(c).isNull).reduce(_ || _))
      .select(resultColumns: _*)
  }

}
