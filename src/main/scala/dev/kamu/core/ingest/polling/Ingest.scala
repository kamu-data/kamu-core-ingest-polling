/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.core.ingest.polling

import java.sql.Timestamp
import java.time.Instant
import java.util.zip.ZipInputStream

import dev.kamu.core.ingest.polling.convert.{
  ConversionStepFactory,
  IngestCheckpoint
}
import dev.kamu.core.ingest.polling.merge.MergeStrategy
import dev.kamu.core.ingest.polling.poll.{
  CacheableSource,
  CachingBehavior,
  DownloadCheckpoint,
  SourceFactory
}
import dev.kamu.core.ingest.polling.prep.{PrepCheckpoint, PrepStepFactory}
import dev.kamu.core.ingest.polling.utils.DFUtils._
import dev.kamu.core.ingest.polling.utils.{
  CheckpointingExecutor,
  ExecutionResult,
  ZipFiles
}
import dev.kamu.core.manifests.{
  DatasetVocabulary,
  ReaderKind,
  RootPollingSource
}
import dev.kamu.core.manifests.parsing.pureconfig.yaml
import yaml.defaults._
import pureconfig.generic.auto._
import dev.kamu.core.utils.fs._
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
  private val sourceFactory = new SourceFactory(fileSystem, getSystemTime)
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
      val source = task.datasetToIngest.rootPollingSource.get
      val cachingBehavior = sourceFactory.getCachingBehavior(source.fetch)

      for (externalSource <- sourceFactory.getSource(source.fetch)) {
        logger.info(
          s"Processing data source: ${task.datasetToIngest.id}:${externalSource.sourceID}"
        )

        val downloadCheckpointPath = task.checkpointsPath
          .resolve(externalSource.sourceID)
          .resolve(AppConf.downloadCheckpointFileName)
        val downloadDataPath = task.pollCachePath
          .resolve(externalSource.sourceID)
          .resolve(AppConf.downloadDataFileName)
        val prepCheckpointPath = task.checkpointsPath
          .resolve(externalSource.sourceID)
          .resolve(AppConf.prepCheckpointFileName)
        val prepDataPath = task.pollCachePath
          .resolve(externalSource.sourceID)
          .resolve(AppConf.prepDataFileName)
        val ingestCheckpointPath = task.checkpointsPath
          .resolve(externalSource.sourceID)
          .resolve(AppConf.ingestCheckpointFileName)
        val ingestDataPath = task.dataPath

        Seq(
          downloadCheckpointPath,
          downloadDataPath,
          prepCheckpointPath,
          prepDataPath,
          ingestCheckpointPath,
          ingestDataPath
        ).map(_.getParent)
          .filter(!fileSystem.exists(_))
          .foreach(fileSystem.mkdirs)

        logger.info(s"Stage: polling")
        val downloadResult = maybeDownload(
          source,
          externalSource,
          cachingBehavior,
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
          ingestDataPath,
          DatasetVocabulary()
        )

        if (ingestResult.wasUpToDate) {
          logger.info(
            s"Data is up to date: ${task.datasetToIngest.id}:${externalSource.sourceID}"
          )
        } else {
          logger.info(
            s"Data was updated: ${task.datasetToIngest.id}:${externalSource.sourceID}"
          )
        }
      }
    }

    logger.info(s"Finished ingest run")
  }

  def maybeDownload(
    source: RootPollingSource,
    externalSource: CacheableSource,
    cachingBehavior: CachingBehavior,
    downloadCheckpointPath: Path,
    downloadDataPath: Path
  ): ExecutionResult[DownloadCheckpoint] = {
    downloadExecutor.execute(
      checkpointPath = downloadCheckpointPath,
      execute = storedCheckpoint => {
        val downloadResult = externalSource.maybeDownload(
          storedCheckpoint,
          cachingBehavior,
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
            && storedCheckpoint.get.downloadTimestamp == downloadCheckpoint.lastDownloaded) {
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
              eventTime = downloadCheckpoint.eventTime,
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
    ingestDataPath: Path,
    vocab: DatasetVocabulary
  ): ExecutionResult[IngestCheckpoint] = {
    ingestExecutor.execute(
      checkpointPath = ingestCheckpointPath,
      execute = storedCheckpoint => {
        if (storedCheckpoint.isDefined
            && storedCheckpoint.get.prepTimestamp == prepCheckpoint.lastPrepared) {
          ExecutionResult(
            wasUpToDate = true,
            checkpoint = storedCheckpoint.get
          )
        } else {
          ingest(
            getSparkSubSession(sparkSession),
            source,
            prepCheckpoint.eventTime,
            prepDataPath,
            ingestDataPath,
            vocab
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
    eventTime: Option[Instant],
    filePath: Path,
    outPath: Path,
    vocab: DatasetVocabulary
  ): Unit = {
    logger.info(
      s"Ingesting the data: in=$filePath, out=$outPath, format=${source.read}"
    )

    // Needed to make Spark re-read files that might've changed between ingest runs
    spark.sqlContext.clearCache()

    val reader = source.read match {
      case _: ReaderKind.Shapefile =>
        readShapefile _
      case _: ReaderKind.Geojson =>
        readGeoJSON _
      case _ =>
        readGeneric _
    }

    reader(spark, source, filePath, vocab)
      .transform(checkForErrors(vocab))
      .transform(normalizeSchema(source))
      .transform(preprocess(source))
      .transform(mergeWithExisting(source, eventTime, outPath, vocab))
      .maybeTransform(source.coalesce != 0, _.coalesce(source.coalesce))
      .write
      .mode(SaveMode.Append)
      .parquet(outPath.toString)
  }

  def readGeneric(
    spark: SparkSession,
    source: RootPollingSource,
    filePath: Path,
    vocab: DatasetVocabulary
  ): DataFrame = {
    val fmt = source.read.asGeneric().asInstanceOf[ReaderKind.Generic]
    val reader = spark.read

    if (fmt.schema.nonEmpty) {
      val fullSchema = fmt.schema :+ s"${vocab.corruptRecordColumn} STRING"
      reader.schema(fullSchema.mkString(", "))
    }

    reader
      .format(fmt.name)
      .options(fmt.options)
      .option("mode", "PERMISSIVE")
      .option("columnNameOfCorruptRecord", vocab.corruptRecordColumn)
      .load(filePath.toString)
  }

  // TODO: This is inefficient
  def readShapefile(
    spark: SparkSession,
    source: RootPollingSource,
    filePath: Path,
    vocab: DatasetVocabulary
  ): DataFrame = {
    val fmt = source.read.asInstanceOf[ReaderKind.Shapefile]

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
    filePath: Path,
    vocab: DatasetVocabulary
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

  def checkForErrors(vocab: DatasetVocabulary)(df: DataFrame): DataFrame = {
    df.getColumn(vocab.corruptRecordColumn) match {
      case None =>
        df
      case Some(col) =>
        val dfCached = df.cache()
        val corrupt = dfCached.select(col).filter(col.isNotNull)
        if (corrupt.count() > 0) {
          throw new Exception(
            "Corrupt records detected:\n" + corrupt
              .showString(numRows = 20, truncate = 0)
          )
        } else {
          dfCached.drop(col)
        }
    }
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

  def mergeWithExisting(
    source: RootPollingSource,
    eventTime: Option[Instant],
    outPath: Path,
    vocab: DatasetVocabulary
  )(
    curr: DataFrame
  ): DataFrame = {
    val spark = curr.sparkSession
    val mergeStrategy = MergeStrategy(source.merge, vocab)

    val prev =
      if (fileSystem.exists(outPath))
        Some(spark.read.parquet(outPath.toString))
      else
        None

    mergeStrategy.merge(
      prev,
      curr,
      getSystemTime(),
      eventTime.map(Timestamp.from)
    )
  }

  def getSparkSubSession(sparkSession: SparkSession): SparkSession = {
    val subSession = sparkSession.newSession()
    GeoSparkSQLRegistrator.registerAll(subSession)
    subSession
  }

}
