import java.sql.Timestamp

import org.scalatest.FunSuite


class TimeSeriesUtilsTest
  extends FunSuite
  with DataFrameSuiteBaseEx
{
  import spark.implicits._

  protected override val enableHiveSupport = false

  def ts(milis: Long) = new Timestamp(milis)

  test("timeSeriesToSnapshot") {
    val series = sc.parallelize(Seq(
      (ts(0), "added", 1, "A", "x"),
      (ts(0), "added", 2, "B", "y"),
      (ts(0), "added", 3, "C", "z"),
      (ts(1), "changed", 1, "A", "a"),
      (ts(1), "changed", 2, "B", "b"),
      (ts(2), "removed", 1, "A", "a"),
      (ts(2), "changed", 2, "B", "bb"),
      (ts(3), "added", 4, "D", "d")
    )).toDF("systemTime", "observed", "id", "name", "data")

    val actual = TimeSeriesUtils
      .timeSeriesToSnapshot(series, "id")
      .orderBy("id")

    val expected = sc.parallelize(Seq(
      (2, "B", "bb", ts(2)),
      (3, "C", "z", ts(0)),
      (4, "D", "d", ts(3))
    )).toDF("id", "name", "data", "lastUpdatedSys")

    assertDataFrameEquals(expected, actual, ignoreNullable = true)
  }

}
