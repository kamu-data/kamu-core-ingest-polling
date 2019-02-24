import org.apache.spark.sql.{Column, DataFrame, RelationalGroupedDataset}

object DFUtils {

  implicit class DataFrameEx(val df: DataFrame) {

    /** Get column if exists
      *
      * TODO: Will not work with nested columns
      */
    def getColumn(column: String): Option[Column] = {
      if (df.columns.contains(column))
        Some(df(column))
      else
        None
    }
  }

  implicit class RelationalGroupedDatasetEx(val df: RelationalGroupedDataset) {

    /** Fixes variadic argument passing */
    def aggv(columns: Column*): DataFrame = {
      val head :: tail = columns
      df.agg(head, tail: _*)
    }
  }

}
