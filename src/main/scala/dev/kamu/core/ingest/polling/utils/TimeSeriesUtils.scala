package dev.kamu.core.ingest.polling.utils

import java.sql.Timestamp

import dev.kamu.core.manifests.DatasetVocabulary
import dev.kamu.core.ingest.polling.utils.DFUtils._
import org.apache.spark.sql.functions.{col, last, lit}
import org.apache.spark.sql.{DataFrame, Row}

object TimeSeriesUtils {

  def empty(proto: DataFrame): DataFrame = {
    val spark = proto.sparkSession

    spark.createDataFrame(spark.sparkContext.emptyRDD[Row], proto.schema)
  }

  /** Projects time series data onto specific point in system time.
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
  def asOf(
    series: DataFrame,
    primaryKey: Seq[String],
    systemTime: Option[Timestamp] = None,
    vocab: DatasetVocabulary = DatasetVocabulary()
  ): DataFrame = {
    val pk = primaryKey.toVector

    def aggAlias(c: String) = "__" + c

    val dataColumns = series.columns
      .filter(!pk.contains(_))
      .toList

    val aggregates = dataColumns
      .map(c => last(c).as(aggAlias(c)))

    val resultColumns = pk.map(col) ++ (
      dataColumns
        .filter(_ != vocab.observationColumn)
        .filter(_ != vocab.systemTimeColumn)
        .map(c => col(aggAlias(c)).as(c))
        :+
          col(aggAlias(vocab.systemTimeColumn))
            .as(vocab.lastUpdatedTimeSystemColumn)
    )

    series
      .when(_ => systemTime.isDefined)(
        _.filter(col(vocab.systemTimeColumn) <= systemTime.get)
      )
      .orderBy(vocab.systemTimeColumn)
      .groupBy(pk.map(col): _*)
      .aggv(aggregates: _*)
      .filter(col(aggAlias(vocab.observationColumn)) =!= lit(vocab.obsvRemoved))
      .select(resultColumns: _*)
  }

  implicit class When[A](a: A) {
    def when(f: A => Boolean)(g: A => A): A = if (f(a)) g(a) else a
  }

}
