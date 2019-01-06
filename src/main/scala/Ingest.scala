import java.nio.file.{Files, Path}

import org.apache.log4j.LogManager
import org.apache.spark.sql.{SaveMode, SparkSession}

class Ingest(config: AppConfig) {
  val logger = LogManager.getLogger(getClass.getName)

  val fileCache = new FileCache()

  val compression = new Compression()

  def pollAndIngest(): Unit = {
    logger.info(s"Starting ingest")
    logger.info(s"Running with config: $config")

    val spark = SparkSession.builder
      .appName("ingest.polling")
      .getOrCreate()

    for (source <- config.sources) {
      logger.info(s"Processing source: ${source.id}")

      val downloadPath = config.downloadDir
        .resolve(source.id)
        .resolve("data.bin")

      val compressedPath = config.downloadDir
        .resolve(source.id)
        .resolve("data.gz")

      val ingestedPath = config.dataDir
        .resolve(source.id)

      val downloadResult = fileCache.maybeDownload(source.url, downloadPath)

      if (!downloadResult.wasUpToDate || !Files.exists(compressedPath)) {
        compression.process(source, downloadPath, compressedPath)
      }

      if (!Files.exists(ingestedPath)) {
        ingest(createSparkSubSession(spark), source, compressedPath, ingestedPath)
      }
    }

    logger.info(s"Finished ingest run")
    spark.close()
  }

  def ingest(spark: SparkSession, source: Source, filePath: Path, outPath: Path): Unit = {
    logger.info(s"Reading the data: in=$filePath, out=$outPath")

    val reader = spark.read

    if (source.schema.nonEmpty)
      reader.schema(source.schema.mkString(", "))

    val df = reader
      .format(source.format)
      .options(source.readerOptions)
      .load(filePath.toString)

    df.write
      .mode(SaveMode.Append)
      .parquet(outPath.toString)
  }

  def createSparkSubSession(sparkSession: SparkSession): SparkSession = {
    sparkSession.newSession()
  }

}
