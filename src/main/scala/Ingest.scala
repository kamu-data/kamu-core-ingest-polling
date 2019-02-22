import FSUtils._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.LogManager
import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession, functions}
import org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator


class Ingest(config: AppConfig) {
  val logger = LogManager.getLogger(getClass.getName)

  val fileSystem = FileSystem.get(hadoopConf)
  val fileCache = new FileCache(fileSystem)
  val compression = new Compression(fileSystem)
  val processing = new Processing()

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

      val downloadResult = fileCache.maybeDownload(source.url, cachePath, body => {
        val extracted = compression.getExtractedStream(source, body)

        if (!fileSystem.exists(downloadPath.getParent))
          fileSystem.mkdirs(downloadPath.getParent)

        val outputStream = fileSystem.create(downloadPath)
        val compressed = compression.toCompressedStream(outputStream)

        processing.process(source, extracted, compressed)
      })

      if (!downloadResult.wasUpToDate) {
        ingest(getSparkSubSession(sparkSession), source, downloadPath, ingestedPath)
      }
    }

    logger.info(s"Finished ingest run")
  }

  def ingest(spark: SparkSession, source: IngestSource, filePath: Path, outPath: Path): Unit = {
    logger.info(s"Ingesting the data: in=$filePath, out=$outPath")

    val dataFrameRaw = source.format.toLowerCase match {
      case "geojson" =>
        readGeoJSON(spark, source, filePath)
      case "worldbank-csv" =>
        readWorldbankCSV(spark, source, filePath)
      case _ =>
        readGeneric(spark, source, filePath)
    }

    val dataFrame = normalizeSchema(dataFrameRaw, source)

    dataFrame.write
      .mode(SaveMode.Append)
      .parquet(outPath.toString)
  }

  def readGeneric(spark: SparkSession, source: IngestSource, filePath: Path): DataFrame = {
    val reader = spark.read

    if (source.schema.nonEmpty)
      reader.schema(source.schema.mkString(", "))

    reader
      .format(source.format)
      .options(source.readerOptions)
      .load(filePath.toString)
  }

  // TODO: This is very inefficient, should extend GeoSpark to support this
  def readGeoJSON(spark: SparkSession, source: IngestSource, filePath: Path): DataFrame = {
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
  def readWorldbankCSV(spark: SparkSession, source: IngestSource, filePath: Path): DataFrame = {
    readGeneric(
      spark,
      source.copy(format="csv"),
      filePath)
  }

  def normalizeSchema(df: DataFrame, source: IngestSource): DataFrame = {
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

  def sparkConf: SparkConf = {
    new SparkConf()
      .setAppName("ingest.polling")
  }

  def hadoopConf: org.apache.hadoop.conf.Configuration = {
    SparkHadoopUtil.get.newConfiguration(sparkConf)
  }

  def sparkSession: SparkSession = {
    SparkSession.builder
      .config(sparkConf)
      .getOrCreate()
  }

  def getSparkSubSession(sparkSession: SparkSession): SparkSession = {
    val subSession = sparkSession.newSession()
    GeoSparkSQLRegistrator.registerAll(subSession)
    subSession
  }

}
