package dev.kamu.core.ingest.polling

import java.sql.Timestamp
import java.util.zip.ZipInputStream

import DFUtils._
import dev.kamu.core.manifests.utils.fs._
import dev.kamu.core.manifests.RootPollingSource
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.LogManager
import org.apache.spark.sql._
import org.datasyslab.geospark.formatMapper.GeoJsonReader
import org.datasyslab.geospark.formatMapper.shapefileParser.ShapefileReader
import org.datasyslab.geosparksql.utils.{Adapter, GeoSparkSQLRegistrator}

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

    for (ds <- config.datasets) {
      logger.info(s"Processing dataset: ${ds.id}")

      val downloadPath = config.volumeMap.downloadDir
        .resolve(ds.id.toString)
        .resolve(s"data.${compression.fileExtension}")

      val cachePath = config.volumeMap.checkpointDir
        .resolve(ds.id.toString)

      val ingestedPath =
        config.volumeMap.dataDirRoot.resolve(ds.id.toString)

      val source = ds.rootPollingSource.get

      val downloadResult =
        maybeDownload(source, cachePath, downloadPath)

      if (!downloadResult.wasUpToDate) {
        ingest(
          getSparkSubSession(sparkSession),
          source,
          downloadPath,
          ingestedPath
        )
      }
    }

    logger.info(s"Finished ingest run")
  }

  def maybeDownload(
    source: RootPollingSource,
    cachePath: Path,
    downloadPath: Path
  ): DownloadResult = {
    cachingDownloader.maybeDownload(
      source.url,
      cachePath,
      body => {
        val extracted = compression.getExtractedStream(source, body)

        if (!fileSystem.exists(downloadPath.getParent))
          fileSystem.mkdirs(downloadPath.getParent)

        val outputStream = fileSystem.create(downloadPath)
        val compressed = compression.toCompressedStream(outputStream)

        processing.process(source, extracted, compressed)
      }
    )
  }

  def ingest(
    spark: SparkSession,
    source: RootPollingSource,
    filePath: Path,
    outPath: Path
  ): Unit = {
    logger.info(s"Ingesting the data: in=$filePath, out=$outPath")

    val reader = source.format.toLowerCase match {
      case "shapefile" =>
        readShapefile _
      case "geojson" =>
        readGeoJSON _
      case "worldbank-csv" =>
        readWorldbankCSV _
      case _ =>
        readGeneric _
    }

    reader(spark, source, filePath)
      .transform(normalizeSchema(source))
      .transform(preprocess(source))
      .transform(mergeWithExisting(source, outPath))
      .maybeTransform(source.coalesce != 0, _.coalesce(source.coalesce))
      .write
      .mode(SaveMode.Append)
      .parquet(outPath.toString)
  }

  def readGeneric(
    spark: SparkSession,
    source: RootPollingSource,
    filePath: Path
  ): DataFrame = {
    val reader = spark.read

    if (source.schema.nonEmpty)
      reader.schema(source.schema.mkString(", "))

    reader
      .format(source.format)
      .options(source.readerOptions)
      .load(filePath.toString)
  }

  // TODO: This is inefficient
  def readShapefile(
    spark: SparkSession,
    source: RootPollingSource,
    filePath: Path
  ): DataFrame = {
    val extractedPath = filePath.getParent.resolve("shapefile")

    val inputStream = fileSystem.open(filePath)
    val bzip2Stream = new BZip2CompressorInputStream(inputStream)
    val zipStream = new ZipInputStream(bzip2Stream)

    ZipFiles.extractZipFile(
      fileSystem,
      zipStream,
      extractedPath,
      source.subPathRegex
    )

    zipStream.close()

    val rdd = ShapefileReader.readToGeometryRDD(
      spark.sparkContext,
      extractedPath.toString
    )

    Adapter
      .toDf(rdd, spark)
      .withColumn(
        "geometry",
        functions.callUDF("ST_GeomFromWKT", functions.col("geometry"))
      )
  }

  // TODO: This is very inefficient, should extend GeoSpark to support this
  def readGeoJSON(
    spark: SparkSession,
    source: RootPollingSource,
    filePath: Path
  ): DataFrame = {
    val rdd = GeoJsonReader.readToGeometryRDD(
      spark.sparkContext,
      filePath.toString,
      false,
      false
    )

    Adapter
      .toDf(rdd, spark)
      .withColumn(
        "geometry",
        functions.callUDF("ST_GeomFromWKT", functions.col("geometry"))
      )
  }

  // TODO: Replace with generic options to skip N lines
  def readWorldbankCSV(
    spark: SparkSession,
    source: RootPollingSource,
    filePath: Path
  ): DataFrame = {
    readGeneric(spark, source.copy(format = "csv"), filePath)
  }

  def normalizeSchema(source: RootPollingSource)(df: DataFrame): DataFrame = {
    if (source.schema.nonEmpty)
      return df

    var result = df
    for (col <- df.columns) {
      result = result.withColumnRenamed(
        col,
        col
          .replaceAll("[ ,;{}\\(\\)=]", "_")
          .replaceAll("[\\n\\r\\t]", "")
      )
    }
    result
  }

  def preprocess(source: RootPollingSource)(df: DataFrame): DataFrame = {
    if (source.preprocess.isEmpty)
      return df

    val spark = df.sparkSession

    df.createTempView("input")

    for (step <- source.preprocess) {
      val tempResult = spark.sql(step.query)
      if (step.view == "output")
        return tempResult
      else
        tempResult.createTempView(s"`${step.view}`")
    }

    throw new RuntimeException(
      "Pre-processing steps do not contain output query"
    )
  }

  def mergeWithExisting(source: RootPollingSource, outPath: Path)(
    curr: DataFrame
  ): DataFrame = {
    val spark = curr.sparkSession
    val mergeStrategy = MergeStrategy(source.mergeStrategy)

    val prev =
      if (fileSystem.exists(outPath))
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
