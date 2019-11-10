package dev.kamu.core.ingest.polling.merge

import java.sql.Timestamp

import dev.kamu.core.ingest.polling.utils.DFUtils._
import dev.kamu.core.ingest.polling.utils.TimeSeriesUtils
import dev.kamu.core.manifests.DatasetVocabulary
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit

/** Append merge strategy.
  *
  * Under this strategy polled data will be appended in its original form
  * to the already ingested data. Optionally can add the system time column.
  *
  * @param vocab vocabulary of system column names and common values
  */
class AppendMergeStrategy(
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

    curr.select(resultColumns: _*)
  }

}
