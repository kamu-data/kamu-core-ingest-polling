package dev.kamu.core.ingest.polling

import java.sql.Timestamp
import java.time.Instant
import java.util.zip.ZipInputStream

import dev.kamu.core.ingest.polling.convert.{
  ConversionStepFactory,
  IngestCheckpoint
}
import dev.kamu.core.ingest.polling.merge.MergeStrategy
import dev.kamu.core.ingest.polling.poll.{DownloadCheckpoint, SourceFactory}
import dev.kamu.core.ingest.polling.prep.{PrepCheckpoint, PrepStepFactory}
import dev.kamu.core.ingest.polling.utils.DFUtils._
import dev.kamu.core.ingest.polling.utils.{
  CheckpointingExecutor,
  ExecutionResult,
  ZipFiles
}
import dev.kamu.core.manifests.parsing.pureconfig.yaml
import yaml.defaults._
import pureconfig.generic.auto._
import dev.kamu.core.manifests.utils.fs._
import dev.kamu.core.manifests._
import org.apache.commons.compress.compressors.bzip2.{
  BZip2CompressorInputStream,
  BZip2CompressorOutputStream
}
import org.apache.commons.io.IOUtils
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
  private val sourceFactory = new SourceFactory(fileSystem)
  private val prepStepFactory = new PrepStepFactory(fileSystem)
  private val conversionStepFactory = new ConversionStepFactory()
  private val downloadExecutor =
    new CheckpointingExecutor[DownloadCheckpoint](fileSystem)
  private val prepExecutor =
    new CheckpointingExecutor[PrepCheckpoint](fileSystem)
  private val ingestExecutor =
    new CheckpointingExecutor[IngestCheckpoint](fileSystem)
  private lazy val sparkSession = getSparkSession()

  def pollAndIngest(): Unit = {
    logger.info(s"Starting ingest")
    logger.info(s"Running with config: $config")

    for (task <- config.tasks) {
      logger.info(s"Processing dataset: ${task.datasetToIngest.id}")

      val downloadCheckpointPath =
        task.checkpointsPath.resolve(AppConf.downloadCheckpointFileName)
      val downloadDataPath =
        task.pollCachePath.resolve(AppConf.downloadDataFileName)

      val prepCheckpointPath =
        task.checkpointsPath.resolve(AppConf.prepCheckpointFileName)
      val prepDataPath = task.pollCachePath.resolve(AppConf.prepDataFileName)

      val ingestCheckpointPath =
        task.checkpointsPath.resolve(AppConf.ingestCheckpointFileName)
      val ingestDataPath = task.dataPath

      val source = task.datasetToIngest.rootPollingSource.get

      Seq(task.pollCachePath, task.checkpointsPath, task.dataPath.getParent)
        .filter(!fileSystem.exists(_))
        .foreach(fileSystem.mkdirs)

      logger.info(s"Stage: polling")

      val downloadResult = maybeDownload(
        source,
        downloadCheckpointPath,
        downloadDataPath
      )

      logger.info(s"Stage: prep")
      val prepResult = maybePrepare(
        source,
        downloadDataPath,
        downloadResult.checkpoint,
        prepCheckpointPath,
        prepDataPath
      )

      logger.info(s"Stage: ingest")
      val ingestResult = maybeIngest(
        source,
        prepResult.checkpoint,
        prepDataPath,
        ingestCheckpointPath,
        ingestDataPath
      )

      if (ingestResult.wasUpToDate) {
        logger.info(s"Dataset is up to date: ${task.datasetToIngest.id}")
      } else {
        logger.info(s"Dataset was updated: ${task.datasetToIngest.id}")
      }
    }

    logger.info(s"Finished ingest run")
  }

  def maybeDownload(
    source: RootPollingSource,
    downloadCheckpointPath: Path,
    downloadDataPath: Path
  ): ExecutionResult[DownloadCheckpoint] = {
    downloadExecutor.execute(
      checkpointPath = downloadCheckpointPath,
      execute = storedCheckpoint => {
        if (storedCheckpoint.isDefined
            && !storedCheckpoint.get.isCacheable
            && fileSystem.exists(downloadDataPath)) {
          logger.warn(s"Skipping uncachable source")
          ExecutionResult(wasUpToDate = true, checkpoint = storedCheckpoint.get)
        } else {
          val dataSource = sourceFactory.getSource(source.fetch)

          val downloadResult = dataSource.maybeDownload(
            storedCheckpoint,
            body => {
              val outputStream = fileSystem.create(downloadDataPath, true)
              val compressedStream =
                new BZip2CompressorOutputStream(outputStream)
              try {
                IOUtils.copy(body, compressedStream)
              } finally {
                compressedStream.close()
              }
            }
          )

          if (!downloadResult.checkpoint.isCacheable)
            logger.warn(
              "Data source is uncacheable - data will not be updated in future."
            )

          downloadResult
        }
      }
    )
  }

  // TODO: Avoid copying data if prepare step is a no-op
  def maybePrepare(
    source: RootPollingSource,
    downloadDataPath: Path,
    downloadCheckpoint: DownloadCheckpoint,
    prepCheckpointPath: Path,
    prepDataPath: Path
  ): ExecutionResult[PrepCheckpoint] = {
    prepExecutor.execute(
      checkpointPath = prepCheckpointPath,
      execute = storedCheckpoint => {
        if (storedCheckpoint.isDefined
            && storedCheckpoint.get.downloadTimestamp == downloadCheckpoint.lastDownloaded
            && fileSystem.exists(prepDataPath)) {
          ExecutionResult(
            wasUpToDate = true,
            checkpoint = storedCheckpoint.get
          )
        } else {
          val prepStep = prepStepFactory.getComposedSteps(source.prepare)
          val convertStep = conversionStepFactory.getComposedSteps(source.read)

          val inputStream = fileSystem.open(downloadDataPath)
          val decompressedInStream = new BZip2CompressorInputStream(inputStream)

          val outputStream = fileSystem.create(prepDataPath, true)
          val compressedOutStream =
            new BZip2CompressorOutputStream(outputStream)

          try {
            val preparedInStream = prepStep.prepare(decompressedInStream)
            val convertedInStream = convertStep.convert(preparedInStream)

            IOUtils.copy(convertedInStream, compressedOutStream)

            prepStep.join()
          } finally {
            decompressedInStream.close()
            compressedOutStream.close()
          }

          ExecutionResult(
            wasUpToDate = false,
            checkpoint = PrepCheckpoint(
              downloadTimestamp = downloadCheckpoint.lastDownloaded,
              lastPrepared = Instant.now()
            )
          )
        }
      }
    )
  }

  def maybeIngest(
    source: RootPollingSource,
    prepCheckpoint: PrepCheckpoint,
    prepDataPath: Path,
    ingestCheckpointPath: Path,
    ingestDataPath: Path
  ): ExecutionResult[IngestCheckpoint] = {
    ingestExecutor.execute(
      checkpointPath = ingestCheckpointPath,
      execute = storedCheckpoint => {
        if (storedCheckpoint.isDefined
            && storedCheckpoint.get.prepTimestamp == prepCheckpoint.lastPrepared
            && fileSystem.exists(ingestDataPath)) {
          ExecutionResult(
            wasUpToDate = true,
            checkpoint = storedCheckpoint.get
          )
        } else {
          ingest(
            getSparkSubSession(sparkSession),
            source,
            prepDataPath,
            ingestDataPath
          )

          ExecutionResult(
            wasUpToDate = false,
            checkpoint = IngestCheckpoint(
              prepTimestamp = prepCheckpoint.lastPrepared,
              lastIngested = Instant.now()
            )
          )
        }
      }
    )
  }

  def ingest(
    spark: SparkSession,
    source: RootPollingSource,
    filePath: Path,
    outPath: Path
  ): Unit = {
    logger.info(
      s"Ingesting the data: in=$filePath, out=$outPath, format=${source.read}"
    )

    val reader = source.read match {
      case _: ReaderShapefile =>
        readShapefile _
      case _: ReaderGeojson =>
        readGeoJSON _
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
    val fmt = source.read.asGeneric().asInstanceOf[ReaderGeneric]
    val reader = spark.read

    if (fmt.schema.nonEmpty)
      reader.schema(fmt.schema.mkString(", "))

    reader
      .format(fmt.name)
      .options(fmt.options)
      .load(filePath.toString)
  }

  // TODO: This is inefficient
  def readShapefile(
    spark: SparkSession,
    source: RootPollingSource,
    filePath: Path
  ): DataFrame = {
    val fmt = source.read.asInstanceOf[ReaderShapefile]

    val extractedPath = filePath.getParent.resolve("shapefile")

    val inputStream = fileSystem.open(filePath)
    val bzip2Stream = new BZip2CompressorInputStream(inputStream)
    val zipStream = new ZipInputStream(bzip2Stream)

    ZipFiles.extractZipFile(
      fileSystem,
      zipStream,
      extractedPath,
      fmt.subPathRegex
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

  def normalizeSchema(source: RootPollingSource)(df: DataFrame): DataFrame = {
    if (source.read.schema.nonEmpty)
      return df

    var result = df
    for (col <- df.columns) {
      result = result.withColumnRenamed(
        col,
        col
          .replaceAll("[ ,;{}()=]", "_")
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
    val mergeStrategy = MergeStrategy(source.merge)

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
