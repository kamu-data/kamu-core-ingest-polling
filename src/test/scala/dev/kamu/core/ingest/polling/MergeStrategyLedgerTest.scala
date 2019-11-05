package dev.kamu.core.ingest.polling

import java.sql.Timestamp

import org.scalatest.FunSuite

class MergeStrategyLedgerTest extends FunSuite with DataFrameSuiteBaseEx {
  import spark.implicits._

  protected override val enableHiveSupport = false

  def ts(milis: Long) = new Timestamp(milis)

  test("From empty") {
    val curr = sc
      .parallelize(
        Seq(
          (ts(0), 1, "a"),
          (ts(1), 2, "b"),
          (ts(2), 3, "c")
        )
      )
      .toDF("eventTime", "id", "data")

    val strategy = new LedgerMergeStrategy(Vector("id"))

    val actual = strategy
      .merge(None, curr, ts(3))
      .orderBy("systemTime", "eventTime", "id")

    val expected = sc
      .parallelize(
        Seq(
          (ts(3), ts(0), 1, "a"),
          (ts(3), ts(1), 2, "b"),
          (ts(3), ts(2), 3, "c")
        )
      )
      .toDF("systemTime", "eventTime", "id", "data")

    assertDataFrameEquals(expected, actual, ignoreNullable = true)
  }

  test("Append existing") {
    val prev = sc
      .parallelize(
        Seq(
          (ts(3), ts(0), 1, "a"),
          (ts(3), ts(1), 2, "b"),
          (ts(3), ts(2), 3, "c")
        )
      )
      .toDF("systemTime", "eventTime", "id", "data")

    val curr = sc
      .parallelize(
        Seq(
          (ts(0), 1, "a"),
          (ts(1), 2, "b"),
          (ts(2), 3, "c"),
          (ts(3), 4, "d"),
          (ts(4), 5, "e"),
          (ts(5), 6, "f")
        )
      )
      .toDF("eventTime", "id", "data")

    val strategy = new LedgerMergeStrategy(Vector("id"))

    val actual = strategy
      .merge(Some(prev), curr, ts(6))
      .orderBy("systemTime", "eventTime", "id")

    val expected = sc
      .parallelize(
        Seq(
          (ts(6), ts(3), 4, "d"),
          (ts(6), ts(4), 5, "e"),
          (ts(6), ts(5), 6, "f")
        )
      )
      .toDF("systemTime", "eventTime", "id", "data")

    assertDataFrameEquals(expected, actual, ignoreNullable = true)
  }

  test("New column added") {
    val prev = sc
      .parallelize(
        Seq(
          (ts(3), ts(0), 1, "a"),
          (ts(3), ts(1), 2, "b"),
          (ts(3), ts(2), 3, "c")
        )
      )
      .toDF("systemTime", "eventTime", "id", "data")

    val curr = sc
      .parallelize(
        Seq(
          (ts(0), 1, "a", "?"),
          (ts(1), 2, "b", "?"),
          (ts(2), 3, "c", "?"),
          (ts(3), 4, "x", "d"),
          (ts(4), 5, "y", "e"),
          (ts(5), 6, "z", "f")
        )
      )
      .toDF("eventTime", "id", "extra", "data")

    val strategy = new LedgerMergeStrategy(Vector("id"))

    val actual = strategy
      .merge(Some(prev), curr, ts(6))
      .orderBy("systemTime", "eventTime", "id")

    val expected = sc
      .parallelize(
        Seq(
          (ts(6), ts(3), 4, "d", "x"),
          (ts(6), ts(4), 5, "e", "y"),
          (ts(6), ts(5), 6, "f", "z")
        )
      )
      .toDF("systemTime", "eventTime", "id", "data", "extra")

    assertDataFrameEquals(expected, actual, ignoreNullable = true)
  }

  // TODO: Consider just returning an error, this is a ledger after all
  test("Old column removed") {
    val prev = sc
      .parallelize(
        Seq(
          (ts(3), ts(0), 1, "x", "a"),
          (ts(3), ts(1), 2, "y", "b"),
          (ts(3), ts(2), 3, "z", "c")
        )
      )
      .toDF("systemTime", "eventTime", "id", "extra", "data")

    val curr = sc
      .parallelize(
        Seq(
          (ts(0), 1, "a"),
          (ts(1), 2, "b"),
          (ts(2), 3, "c"),
          (ts(3), 4, "d"),
          (ts(4), 5, "e"),
          (ts(5), 6, "f")
        )
      )
      .toDF("eventTime", "id", "data")

    val strategy = new LedgerMergeStrategy(Vector("id"))

    val actual = strategy
      .merge(Some(prev), curr, ts(6))
      .orderBy("systemTime", "eventTime", "id")

    val expected = sc
      .parallelize(
        Seq(
          (ts(6), ts(3), 4, null, "d"),
          (ts(6), ts(4), 5, null, "e"),
          (ts(6), ts(5), 6, null, "f")
        )
      )
      .toDF("systemTime", "eventTime", "id", "extra", "data")

    assertDataFrameEquals(expected, actual, ignoreNullable = true)
  }

}
