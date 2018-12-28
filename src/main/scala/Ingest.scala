import java.nio.file.Files

import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession

class Ingest(config: AppConfig) {
  val logger = LogManager.getLogger(getClass.getName)

  val fileCache = new FileCache(
    config.downloadDir,
    config.cacheDir)

  val compression = new Compression()

  def pollAndIngest(): Unit = {
    logger.info(s"Starting ingest")

    for (source <- config.sources) {
      logger.info(s"Processing source: ${source.id}")

      val downloadResult = download(source)

      val outPath = config.dataDir.resolve(source.id)

      if (!downloadResult.wasUpToDate || !Files.exists(outPath))
        ingest(source, downloadResult.filePath, outPath.toString)
    }

    logger.info(s"Finished ingest run")
  }

  def download(source: Source): DownloadResult = {
    val res = fileCache.maybeDownload(new CachedFile(
      namespace = source.id,
      url = source.url))

    if (res.wasUpToDate) {
      res
    } else {
      // TODO: avoid transcoding by adding Zip file support to Spark
      val compressionResult = compression.process(res.filePath)
      res.copy(filePath = compressionResult.filePath)
    }
  }

  def ingest(source: Source, filePath: String, outPath: String): Unit = {
    logger.info(s"Reading the data: in=$filePath, out=$outPath")

    val spark = SparkSession.builder
      .appName(source.id)
      .getOrCreate()

    var reader = spark.read
      .format(source.format)
      .schema(Schemas.schemas(source.schemaName))
    reader = source.readerOptions.foldLeft(reader)(
      (r, op) => r.option(op.name, op.value)
    )

    val df = reader.load(filePath)
    df.write.parquet(outPath.toString)

    spark.close()
  }

}
