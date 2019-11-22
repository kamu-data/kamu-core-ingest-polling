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

import dev.kamu.core.manifests.utils.fs._
import dev.kamu.core.manifests.{
  Dataset,
  DatasetID,
  ExternalSourceFetchUrl,
  MergeStrategySnapshot,
  ReaderGeojson,
  RootPollingSource
}
import org.apache.hadoop
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.functions
import org.scalatest.FunSuite

class IngestGeoJSONTest extends FunSuite with DataFrameSuiteBaseEx {
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
    systemTime: Timestamp
  ) = {
    val dsID = DatasetID("com.kamu.test")

    val inputPath = tempDir
      .resolve("src")
      .resolve(UUID.randomUUID.toString + ".json")

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
                fetch = ExternalSourceFetchUrl(url = inputPath.toUri),
                read = ReaderGeojson(),
                merge = MergeStrategySnapshot(
                  primaryKey = Vector("id"),
                  modificationIndicator = None
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

  test("ingest polygons") {
    val inputData =
      """{
        |  "type": "FeatureCollection",
        |  "features": [
        |    {
        |      "type": "Feature",
        |      "properties": {
        |        "id": 0,
        |        "zipcode": "00101",
        |        "name": "A"
        |      },
        |      "geometry": {
        |        "type": "Polygon",
        |        "coordinates": [
        |          [
        |            [0.0, 0.0],
        |            [10.0, 0.0],
        |            [10.0, 10.0],
        |            [0.0, 10.0],
        |            [0.0, 0.0]
        |          ]
        |        ]
        |      }
        |    },
        |    {
        |      "type": "Feature",
        |      "properties": {
        |        "id": 1,
        |        "zipcode": "00202",
        |        "name": "B"
        |      },
        |      "geometry": {
        |        "type": "Polygon",
        |        "coordinates": [
        |          [
        |            [0.0, 0.0],
        |            [20.0, 0.0],
        |            [20.0, 20.0],
        |            [0.0, 20.0],
        |            [0.0, 0.0]
        |          ]
        |        ]
        |      }
        |    }
        |  ]
        |}""".stripMargin

    val expected = sc
      .parallelize(
        Seq(
          (
            ts(0),
            "I",
            "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))",
            "0",
            "00101",
            "A"
          ),
          (
            ts(0),
            "I",
            "POLYGON ((0 0, 20 0, 20 20, 0 20, 0 0))",
            "1",
            "00202",
            "B"
          )
        )
      )
      .toDF("systemTime", "observed", "geometry", "id", "zipcode", "name")
      .withColumn(
        "geometry",
        functions.callUDF("ST_GeomFromWKT", functions.col("geometry"))
      )

    withTempDir(tempDir => {
      val actual = ingest(tempDir, inputData, ts(0))
        .orderBy("id")

      assertDataFrameEquals(expected, actual, ignoreNullable = true)
    })
  }

}
