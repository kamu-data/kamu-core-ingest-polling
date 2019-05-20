package dev.kamu.core.ingest.polling

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.{col, last, lit}
import DFUtils._

case class Vocabulary(
  systemTimeColumn: String = "systemTime",
  lastUpdatedTimeSystemColumn: String = "lastUpdatedSys",
  observationColumn: String = "observed",
  obsvAdded: String = "added",
  obsvChanged: String = "changed",
  obsvRemoved: String = "removed"
)

object TimeSeriesUtils {

  def empty(proto: DataFrame): DataFrame = {
    val spark = proto.sparkSession

    spark.createDataFrame(spark.sparkContext.emptyRDD[Row], proto.schema)
  }

  /** Creates a snapshot from time series data.
    *
    * Time series data is expected to have following columns:
    *   - observation
    *   - systemTime
    *   - specified primary key
    *
    * The system time column is replaced with last updated time column.
    *
    * TODO: Test performance of ORDER BY + LAST() vs ORDER BY DESC + FIRST()
    **/
  def timeSeriesToSnapshot(
    series: DataFrame,
    pkColumn: String,
    vocab: Vocabulary = Vocabulary()
  ): DataFrame = {
    def aggAlias(c: String) = "__" + c

    val dataColumns = series.columns
      .filter(_ != pkColumn)
      .toList

    val aggregates = dataColumns
      .map(c => last(c).as(aggAlias(c)))

    val resultColumns = col(pkColumn) :: (
      dataColumns
        .filter(_ != vocab.observationColumn)
        .filter(_ != vocab.systemTimeColumn)
        .map(c => col(aggAlias(c)).as(c))
        :+
          col(aggAlias(vocab.systemTimeColumn))
            .as(vocab.lastUpdatedTimeSystemColumn)
    )

    series
      .orderBy(vocab.systemTimeColumn)
      .groupBy(pkColumn)
      .aggv(aggregates: _*)
      .filter(col(aggAlias(vocab.observationColumn)) =!= lit(vocab.obsvRemoved))
      .select(resultColumns: _*)
  }
}
