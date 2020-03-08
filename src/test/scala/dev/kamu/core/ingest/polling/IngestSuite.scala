/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.core.ingest.polling

import java.io.PrintWriter
import java.sql.Timestamp
import java.time.ZoneOffset
import java.util.UUID

import dev.kamu.core.manifests._
import dev.kamu.core.manifests.infra.MetadataChainFS
import dev.kamu.core.utils.ManualClock
import dev.kamu.core.utils.fs._
import dev.kamu.core.utils.test.KamuDataFrameSuite
import org.apache.hadoop
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame
import org.scalatest.Suite

trait IngestSuite extends KamuDataFrameSuite { self: Suite =>
  private val sysTempDir = new Path(System.getProperty("java.io.tmpdir"))
  protected val fileSystem = sysTempDir.getFileSystem(new Configuration())

  def withTempDir(work: Path => Unit): Unit = {
    val testTempDir =
      sysTempDir.resolve("kamu-test-" + UUID.randomUUID.toString)
    fileSystem.mkdirs(testTempDir)

    try {
      work(testTempDir)
    } finally {
      fileSystem.delete(testTempDir, true)
    }
  }

  def writeFile(path: Path, content: String): Unit = {
    fileSystem.mkdirs(path.getParent)
    val writer = new PrintWriter(fileSystem.create(path))
    writer.write(content)
    writer.close()
  }

  def ingest(
    tempDir: Path,
    dataset: DatasetSnapshot,
    systemTime: Timestamp
  ): DataFrame = {
    val systemClock = new ManualClock(
      Some(java.time.Clock.fixed(systemTime.toInstant, ZoneOffset.UTC))
    )

    val metaDir = tempDir.resolve("dataset")
    val metaChain = new MetadataChainFS(fileSystem, metaDir)
    metaChain.init(dataset, systemClock.instant())

    val outputDir = tempDir.resolve("data")

    val conf = AppConf(
      tasks = Vector(
        IngestTask(
          datasetToIngest = dataset.id,
          datasetLayout = DatasetLayout(
            metadataDir = metaDir,
            checkpointsDir = tempDir.resolve("checkpoints"),
            cacheDir = tempDir.resolve("checkpoints"),
            dataDir = outputDir
          )
        )
      )
    )

    val ingest = new Ingest(
      config = conf,
      hadoopConf = new hadoop.conf.Configuration(),
      getSparkSession = () => spark,
      systemClock = systemClock
    )

    ingest.pollAndIngest()

    spark.read.parquet(outputDir.toString)
  }
}
