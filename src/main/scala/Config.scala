import java.net.URI

import org.apache.hadoop.fs.Path
import pureconfig.generic.ProductHint
import pureconfig.module.yaml.loadYamlOrThrow
import pureconfig.{CamelCase, ConfigFieldMapping, ConfigReader}


///////////////////////////////////////////////////////////////////////////////
// Source Config
///////////////////////////////////////////////////////////////////////////////

object SourceConf {
  val DEFAULT_READER_OPTIONS: Map[String, String] = Map(
    "mode" -> "FAILFAST"
  )
}


case class SourceConf(
  id: String,

  url: URI,

  compression: Option[String] = None,

  /** Path to a data file within an archive */
  subPath: Option[Path] = None,

  /** Regex for finding desired data file within an archive */
  subPathRegex: Option[String] = None,

  format: String,

  /** Options to pass into the [[org.apache.spark.sql.DataFrameReader]]
    *
    * Options in config will be merged with [[SourceConf.DEFAULT_READER_OPTIONS]].
    */
  readerOptions: Map[String, String] = Map.empty,

  /** A DDL-formatted schema that can be used to cast values into
    * more appropriate data types.
    */
  schema: Vector[String] = Vector.empty,

  /** Pre-processing steps to shape the data */
  preprocess: Vector[String] = Vector.empty,

  /** One of the supported merge strategies (see [[MergeStrategyConf]]) */
  mergeStrategy: MergeStrategyConf = Append(),

  /** Collapse partitions of the result to specified number
    *
    * If zero - the step will be skipped
    */
  coalesce: Int = 1
)


///////////////////////////////////////////////////////////////////////////////
// Merge Strategies
///////////////////////////////////////////////////////////////////////////////

sealed trait MergeStrategyConf


/** Append merge strategy.
  *
  * See [[AppendMergeStrategy]] class.
  *
  * @param addSystemTime whether to add a system time column to data
  */
case class Append(
  addSystemTime: Boolean = false
) extends MergeStrategyConf


/** Ledger merge strategy.
  *
  * See [[LedgerMergeStrategy]] class.
  * @param primaryKey name of the column that uniquely identifies the
  *                   record throughout its lifetime
  */
case class Ledger(primaryKey: String) extends MergeStrategyConf


/** Snapshot merge strategy.
  *
  * See [[SnapshotMergeStrategy]] class.
  * @param primaryKey name of the column that uniquely identifies the
  *                   record throughout its lifetime
  * @param modificationIndicator name of the column that always has a
  *                              new value when row data changes, for
  *                              example this can be a modification
  *                              timestamp, an incremental version, or a data
  *                              hash. If not specified all data columns will
  *                              be compared one by one.
  */
case class Snapshot(
  primaryKey: String,
  modificationIndicator: Option[String]
) extends MergeStrategyConf


///////////////////////////////////////////////////////////////////////////////
// Application config
///////////////////////////////////////////////////////////////////////////////

case class AppConf(
  /** Directory to store downloaded data in before processing */
  downloadDir: Path,

  /** Directory to store cache information not to re-download
    * data if it didn't change
    */
  checkpointDir: Path,

  /** Root data set directory for ingested raw data */
  dataDir: Path,

  /** List of sources to poll */
  sources: Vector[SourceConf] = Vector.empty
) {
  def withDefaults(): AppConf = {
    copy(
      sources = sources.map(source =>
        source.copy(
          readerOptions = SourceConf.DEFAULT_READER_OPTIONS ++ source.readerOptions)))
  }
}


object AppConf {
  import pureconfig.generic.auto._

  val configFileName = "poll-config.yaml"
  val pollCacheFileName = "poll-cache.json"

  implicit val pathReader = ConfigReader[String]
    .map(s => new Path(URI.create(s)))

  implicit def hint[T]: ProductHint[T] = ProductHint[T](
    ConfigFieldMapping(CamelCase, CamelCase),
    useDefaultArgs = true,
    allowUnknownKeys = false)

  def load(): AppConf = {
    val configStream = getClass.getClassLoader.getResourceAsStream(configFileName)
    if (configStream == null)
      throw new RuntimeException(
        s"Unable to locate $configFileName on classpath")

    val configString = scala.io.Source.fromInputStream(configStream).mkString
    val config = loadYamlOrThrow[AppConf](configString)

    config.withDefaults()
  }


}
