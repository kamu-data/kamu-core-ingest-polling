import FSUtils._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.LogManager
import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession, functions}
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator


class Ingest(config: AppConfig) {
  val logger = LogManager.getLogger(getClass.getName)

  val fileSystem = FileSystem.get(hadoopConf)
  val fileCache = new FileCache(fileSystem)
  val compression = new Compression(fileSystem)

  def pollAndIngest(): Unit = {
    logger.info(s"Starting ingest")
    logger.info(s"Running with config: $config")

    for (source <- config.sources) {
      logger.info(s"Processing source: ${source.id}")

      val downloadPath = config.downloadDir
        .resolve(source.id)
        .resolve("data.bin")

      val cachePath = config.checkpointDir
        .resolve(source.id)

      val compressedPath = config.downloadDir
        .resolve(source.id)
        .resolve("data.gz")

      val ingestedPath = config.dataDir.resolve(source.id)

      val downloadResult = fileCache.maybeDownload(source.url, downloadPath, cachePath)

      if (!downloadResult.wasUpToDate) {
        compression.process(source, downloadPath, compressedPath)

        ingest(getSparkSubSession(sparkSession), source, compressedPath, ingestedPath)
      }
    }

    logger.info(s"Finished ingest run")
  }

  def ingest(spark: SparkSession, source: Source, filePath: Path, outPath: Path): Unit = {
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

    writeGeneric(dataFrame, outPath)
  }

  def readGeneric(spark: SparkSession, source: Source, filePath: Path): DataFrame = {
    val reader = spark.read

    if (source.schema.nonEmpty)
      reader.schema(source.schema.mkString(", "))

    reader
      .format(source.format)
      .options(source.readerOptions)
      .load(filePath.toString)
  }

  def writeGeneric(dataFrame: DataFrame, outPath: Path): Unit = {
    dataFrame.write
      .mode(SaveMode.Append)
      .parquet(outPath.toString)
  }

  // TODO: This is very inefficient, should extend GeoSpark to support this
  def readGeoJSON(spark: SparkSession, source: Source, filePath: Path): DataFrame = {

    val splitPath = filePath.getParent.resolve(
      filePath.getName.replaceAll("\\.gz", ".sjson.gz"))

    logger.info(s"Pre-processing GeoJSON: in=$filePath, out=$splitPath")
    GeoJSON.toMultiLineJSON(fileSystem, filePath, splitPath)

    val df = readGeneric(
      spark,
      source.copy(format = "json"),
      splitPath)

    df.createTempView("df")

    df.withColumn(
      "geometry",
      functions.callUDF("ST_GeomFromGeoJSON", df.col("geojson")))
  }

  // TODO: Replace with generic options to skip N lines
  def readWorldbankCSV(spark: SparkSession, source: Source, filePath: Path): DataFrame = {
    val preprocPath = filePath.getParent.resolve(
      filePath.getName.replaceAll("\\.gz", ".pp.gz"))

    logger.info(s"Pre-processing WorldBankCSV: in=$filePath, out=$preprocPath")
    WorldBank.toPlainCSV(fileSystem, filePath, preprocPath)

    readGeneric(
      spark,
      source.copy(format="csv"),
      preprocPath)
  }

  def normalizeSchema(df: DataFrame, source: Source): DataFrame = {
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
      .set("spark.serializer", classOf[KryoSerializer].getName)
      .set("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
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
