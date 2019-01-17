import java.net.URL
import java.nio.file.Path


object Source {
  val EVENT_TIME_COLUMN = "eventTime"

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
  schema: Vector[String] = Vector.empty,

  /** Name of the column that should be used as event time.
    *
    * If not provided the system will use the ingestion time
    * as event time.
    */
  eventTimeColumn: Option[String],

  /** Format string to use when parsing the [[Source.eventTimeColumn]].
    *
    * Defaults to Zulu time RFC3339 format.
    */
  eventTimeColumnFormat: String = "yyyy-MM-dd'T'HH:mm:ssX"
)


case class AppConfig(
  downloadDir: Path,
  dataDir: Path,
  sources: Vector[Source] = Vector.empty
)
