import java.net.URI
import java.nio.file.{Path, Paths}

import org.apache.log4j.LogManager
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession, functions}
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator

class Ingest(config: AppConfig) {
  val logger = LogManager.getLogger(getClass.getName)

  val fileCache = new FileCache()

  val compression = new Compression()

  def pollAndIngest(): Unit = {
    logger.info(s"Starting ingest")
    logger.info(s"Running with config: $config")

    for (source <- config.sources) {
      logger.info(s"Processing source: ${source.id}")

      val downloadPath = Paths.get(config.downloadDir)
        .resolve(source.id)
        .resolve("data.bin")

      val cachePath = Paths.get(config.checkpointDir)
        .resolve(source.id)

      val compressedPath = Paths.get(config.downloadDir)
        .resolve(source.id)
        .resolve("data.gz")

      // TODO: Can't use NIO Path here since gs:// URLs don't work with it
      val ingestedPath = URI.create(config.dataDir.toString + "/" + source.id)

      val downloadResult = fileCache.maybeDownload(source.url, downloadPath, cachePath)

      if (!downloadResult.wasUpToDate) {
        compression.process(source, downloadPath, compressedPath)

        ingest(getSparkSubSession(sparkSession), source, compressedPath, ingestedPath)
      }
    }

    logger.info(s"Finished ingest run")
  }

  def ingest(spark: SparkSession, source: Source, filePath: Path, outPath: URI): Unit = {
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

  def writeGeneric(dataFrame: DataFrame, outPath: URI): Unit = {
    dataFrame.write
      .mode(SaveMode.Append)
      .parquet(outPath.toString)
  }

  // TODO: This is very inefficient, should extend GeoSpark to support this
  def readGeoJSON(spark: SparkSession, source: Source, filePath: Path): DataFrame = {
    val splitPath = Paths.get(
      filePath.toString.replaceAll("\\.gz", ".sjson.gz"))

    logger.info(s"Pre-processing GeoJSON: in=$filePath, out=$splitPath")
    GeoJSON.toMultiLineJSON(filePath, splitPath)

    val df = readGeneric(
      spark,
      source.copy(format = "json"),
      splitPath)

    df.createTempView("df")

    df.withColumn(
      "geometry",
      functions.callUDF("ST_GeomFromGeoJSON", df.col("geojson")))
  }

  // TODO: Replace with generic options to skin N lines
  def readWorldbankCSV(spark: SparkSession, source: Source, filePath: Path): DataFrame = {
    val preprocPath = Paths.get(
      filePath.toString.replaceAll("\\.gz", ".pp.gz"))

    logger.info(s"Pre-processing WorldBankCSV: in=$filePath, out=$preprocPath")
    WorldBank.toPlainCSV(filePath, preprocPath)

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

  def sparkSession: SparkSession = {
    SparkSession.builder
      .appName("ingest.polling")
      .config("spark.serializer", classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
      .getOrCreate()
  }

  def getSparkSubSession(sparkSession: SparkSession): SparkSession = {
    val subSession = sparkSession.newSession()
    GeoSparkSQLRegistrator.registerAll(subSession)
    subSession
  }

}
