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
import java.util.UUID

import dev.kamu.core.utils.fs._
import dev.kamu.core.utils.test.KamuDataFrameSuite
import dev.kamu.core.manifests.{
  Dataset,
  DatasetID,
  ExternalSourceFetchUrl,
  MergeStrategySnapshot,
  ReaderCsv,
  RootPollingSource
}
import org.apache.hadoop
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.scalatest.FunSuite

class IngestSnapshotTest extends FunSuite with KamuDataFrameSuite {
  import spark.implicits._
  protected override val enableHiveSupport = false

  private val sysTempDir = new Path(System.getProperty("java.io.tmpdir"))
  private val fileSystem = sysTempDir.getFileSystem(new Configuration())

  def ts(milis: Long) = new Timestamp(milis)

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

  private def ingest(
    tempDir: Path,
    inputData: String,
    inputSchema: Vector[String],
    systemTime: Timestamp
  ) = {
    val dsID = DatasetID("com.kamu.test")

    val inputPath = tempDir
      .resolve("src")
      .resolve(UUID.randomUUID.toString + ".csv")

    writeFile(inputPath, inputData)

    val outputDir = tempDir.resolve("data")

    val conf = AppConf(
      tasks = Vector(
        IngestTask(
          checkpointsPath = tempDir.resolve("checkpoints"),
          pollCachePath = tempDir.resolve("poll"),
          dataPath = outputDir,
          datasetToIngest = Dataset(
            id = dsID,
            rootPollingSource = Some(
              RootPollingSource(
                fetch = ExternalSourceFetchUrl(inputPath.toUri),
                read = ReaderCsv(schema = inputSchema),
                merge = MergeStrategySnapshot(
                  primaryKey = Vector("id"),
                  modificationIndicator = Some("version")
                )
              )
            )
          ).postLoad()
        )
      )
    )

    val ingest = new Ingest(
      config = conf,
      hadoopConf = new hadoop.conf.Configuration(),
      getSparkSession = () => spark,
      getSystemTime = () => systemTime
    )

    ingest.pollAndIngest()

    spark.read.parquet(outputDir.toString)
  }

  test("first time ingest") {
    val inputData =
      """1,alex,100,0
        |2,bob,200,0
        |3,charlie,300,0
        |""".stripMargin

    val inputSchema =
      Vector("id INT", "name STRING", "balance INT", "version INT")

    val expected = sc
      .parallelize(
        Seq(
          (ts(0), "I", 1, "alex", 100, 0),
          (ts(0), "I", 2, "bob", 200, 0),
          (ts(0), "I", 3, "charlie", 300, 0)
        )
      )
      .toDF("systemTime", "observed", "id", "name", "balance", "version")

    withTempDir(tempDir => {
      val actual = ingest(tempDir, inputData, inputSchema, ts(0))
        .orderBy("systemTime", "id")

      assertDataFrameEquals(expected, actual, ignoreNullable = true)
    })
  }

  test("merge with existing") {
    val inputData1 =
      """1,alex,100,0
        |2,bob,200,0
        |3,charlie,300,0
        |""".stripMargin

    val inputData2 =
      """2,bob,200,0
        |3,charlie,500,1
        |4,dan,100,0
        |""".stripMargin

    val inputSchema =
      Vector("id INT", "name STRING", "balance INT", "version INT")

    val expected = sc
      .parallelize(
        Seq(
          (ts(0), "I", 1, "alex", 100, 0),
          (ts(0), "I", 2, "bob", 200, 0),
          (ts(0), "I", 3, "charlie", 300, 0),
          (ts(1), "D", 1, "alex", 100, 0),
          (ts(1), "U", 3, "charlie", 500, 1),
          (ts(1), "I", 4, "dan", 100, 0)
        )
      )
      .toDF("systemTime", "observed", "id", "name", "balance", "version")

    withTempDir(tempDir => {
      ingest(tempDir, inputData1, inputSchema, ts(0))

      val actual = ingest(tempDir, inputData2, inputSchema, ts(1))
        .orderBy("systemTime", "id")

      assertDataFrameEquals(expected, actual, ignoreNullable = true)
    })
  }

}
