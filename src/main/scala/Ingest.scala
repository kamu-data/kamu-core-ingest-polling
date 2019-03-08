import java.sql.Timestamp

import FSUtils._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.LogManager
import org.apache.spark.sql._
import org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator


class Ingest(
  config: AppConf,
  hadoopConf: org.apache.hadoop.conf.Configuration,
  getSparkSession: () => SparkSession,
  getSystemTime: () => Timestamp
) {
  private val logger = LogManager.getLogger(getClass.getName)

  private val fileSystem = FileSystem.get(hadoopConf)
  private lazy val sparkSession = getSparkSession()
  private val cachingDownloader = new CachingDownloader(fileSystem)
  private val compression = new Compression(fileSystem)
  private val processing = new Processing()

  def pollAndIngest(): Unit = {
    logger.info(s"Starting ingest")
    logger.info(s"Running with config: $config")

    for (source <- config.sources) {
      logger.info(s"Processing source: ${source.id}")

      val downloadPath = config.downloadDir
        .resolve(source.id)
        .resolve(s"data.${compression.fileExtension}")

      val cachePath = config.checkpointDir
        .resolve(source.id)

      val ingestedPath = config.dataDir.resolve(source.id)

      val downloadResult = maybeDownload(source, cachePath, downloadPath)

      if (!downloadResult.wasUpToDate) {
        ingest(getSparkSubSession(sparkSession), source, downloadPath, ingestedPath)
      }
    }

    logger.info(s"Finished ingest run")
  }

  def maybeDownload(source: SourceConf, cachePath: Path, downloadPath: Path): DownloadResult = {
    cachingDownloader.maybeDownload(source.url, cachePath, body => {
      val extracted = compression.getExtractedStream(source, body)

      if (!fileSystem.exists(downloadPath.getParent))
        fileSystem.mkdirs(downloadPath.getParent)

      val outputStream = fileSystem.create(downloadPath)
      val compressed = compression.toCompressedStream(outputStream)

      processing.process(source, extracted, compressed)
    })
  }

  def ingest(spark: SparkSession, source: SourceConf, filePath: Path, outPath: Path): Unit = {
    logger.info(s"Ingesting the data: in=$filePath, out=$outPath")

    val dataFrameRaw = source.format.toLowerCase match {
      case "geojson" =>
        readGeoJSON(spark, source, filePath)
      case "worldbank-csv" =>
        readWorldbankCSV(spark, source, filePath)
      case _ =>
        readGeneric(spark, source, filePath)
    }

    val dataFrameNormalized = normalizeSchema(dataFrameRaw, source)

    val dataFramePreprocessed = preprocess(spark, dataFrameNormalized, source)

    val dataFrameMerged = mergeWithExisting(spark, dataFramePreprocessed, source, outPath)

    val dataFrameCoalesced = if (source.coalesce == 0)
      dataFrameMerged
    else
      dataFrameMerged.coalesce(source.coalesce)

    dataFrameCoalesced.write
      .mode(SaveMode.Append)
      .parquet(outPath.toString)
  }

  def readGeneric(spark: SparkSession, source: SourceConf, filePath: Path): DataFrame = {
    val reader = spark.read

    if (source.schema.nonEmpty)
      reader.schema(source.schema.mkString(", "))

    reader
      .format(source.format)
      .options(source.readerOptions)
      .load(filePath.toString)
  }

  // TODO: This is very inefficient, should extend GeoSpark to support this
  def readGeoJSON(spark: SparkSession, source: SourceConf, filePath: Path): DataFrame = {
    val df = readGeneric(
      spark,
      source.copy(format = "json"),
      filePath)

    df.createTempView("df")

    df.withColumn(
      "geometry",
      functions.callUDF("ST_GeomFromGeoJSON", df.col("geojson")))
  }

  // TODO: Replace with generic options to skip N lines
  def readWorldbankCSV(spark: SparkSession, source: SourceConf, filePath: Path): DataFrame = {
    readGeneric(
      spark,
      source.copy(format="csv"),
      filePath)
  }

  def normalizeSchema(df: DataFrame, source: SourceConf): DataFrame = {
    if(source.schema.nonEmpty)
      return df

    var result = df
    for(col <- df.columns) {
      result = result.withColumnRenamed(
        col,
        col.replaceAll("[ ,;{}\\(\\)\\n\\t=]", "_"))
    }
    result
  }

  def preprocess(spark: SparkSession, df: DataFrame, source: SourceConf): DataFrame = {
    if (source.preprocess.isEmpty)
      return df

    df.createTempView("input")
    source.preprocess.foreach(spark.sql)

    spark.sql(s"SELECT * FROM output")
  }

  def mergeWithExisting(spark: SparkSession, curr: DataFrame, source: SourceConf, outPath: Path): DataFrame = {
    val mergeStrategy = MergeStrategy(source.mergeStrategy)

    val prev = if (fileSystem.exists(outPath))
      Some(spark.read.parquet(outPath.toString))
    else
      None

    mergeStrategy.merge(prev, curr, getSystemTime())
  }

  def getSparkSubSession(sparkSession: SparkSession): SparkSession = {
    val subSession = sparkSession.newSession()
    GeoSparkSQLRegistrator.registerAll(subSession)
    subSession
  }

}
