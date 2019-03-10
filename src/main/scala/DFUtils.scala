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

    /** Reorders columns in the [[DataFrame]] */
    def columnToFront(columns: String*): DataFrame = {
      val front = columns.toList
      val back = df.columns.filter(!front.contains(_))
      val newColumns = front ++ back
      val head :: tail = newColumns
      df.select(head, tail: _*)
    }

    /** Reorders columns in the [[DataFrame]] */
    def columnToBack(columns: String*): DataFrame = {
      val back = columns.toList
      val front = df.columns.filter(!back.contains(_)).toList
      val newColumns = front ++ back
      val head :: tail = newColumns
      df.select(head, tail: _*)
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
