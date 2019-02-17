import java.net.URL
import java.nio.file.Path


object Source {
  val DEFAULT_READER_OPTIONS: Map[String, String] = Map(
    "mode" -> "FAILFAST"
  )
}


case class Source(
  id: String,
  url: URL,
  compression: Option[String] = None,
  subPath: Option[Path] = None,
  format: String,

  /** Options to pass into the [[org.apache.spark.sql.DataFrameReader]]
    *
    * Options in config will be merged with [[Source.DEFAULT_READER_OPTIONS]].
    */
  readerOptions: Map[String, String] = Map.empty,

  /** A DDL-formatted schema that can be used to cast values into
    * more appropriate data types.
    */
  schema: Vector[String] = Vector.empty
)


case class AppConfig(
  /** Directory to store downloaded data in before processing */
  downloadDir: Path,

  /** Directory to store cache information not to re-download
    * data if it didn't change
    */
  checkpointDir: Path,

  /** Root data set directory for ingested raw data */
  dataDir: Path,
  sources: Vector[Source] = Vector.empty
)
