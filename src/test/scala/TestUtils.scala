import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{lit, when}
import org.scalatest.Suite

object TestUtils {

  def ignoreNullableSchema(df: DataFrame): DataFrame = {
    // Coalesce makes spark think the column can be nullable
    df.select(
      df.columns.map(c =>
        when(df(c).isNotNull, df(c))
          .otherwise(lit(null))
          .as(c)): _*)
  }
}


trait DataFrameSuiteBaseEx extends DataFrameSuiteBase { self: Suite =>

  def assertDataFrameEquals(expected: DataFrame, actual: DataFrame, ignoreNullable: Boolean): Unit = {
    val exp = if (ignoreNullable) TestUtils.ignoreNullableSchema(expected) else expected
    val act = if (ignoreNullable) TestUtils.ignoreNullableSchema(actual) else actual
    super.assertDataFrameEquals(exp, act)
  }

}
