package dev.kamu.core.ingest.polling

import java.io.PrintWriter
import java.sql.Timestamp
import java.util.UUID

import FSUtils._
import dev.kamu.core.manifests.{
  DataSourcePolling,
  RepositoryVolumeMap,
  Snapshot
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
    val sourceID = "com.kamu.test"

    val inputPath = tempDir
      .resolve("src")
      .resolve(UUID.randomUUID.toString + ".json")

    writeFile(inputPath, inputData)

    val conf = AppConf(
      repository = RepositoryVolumeMap(
        downloadDir = tempDir.resolve("poll"),
        checkpointDir = tempDir.resolve("checkpoint"),
        dataDir = tempDir.resolve("root")
      ),
      sources = Vector(
        DataSourcePolling(
          id = sourceID,
          url = inputPath.toUri,
          format = "geojson",
          mergeStrategy =
            Snapshot(primaryKey = "id", modificationIndicator = None)
        )
      )
    ).withDefaults()

    val ingest = new Ingest(
      config = conf,
      hadoopConf = new hadoop.conf.Configuration(),
      getSparkSession = () => spark,
      getSystemTime = () => systemTime
    )

    ingest.pollAndIngest()

    val outputDir = conf.repository.dataDir.resolve(sourceID)

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
            "added",
            "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))",
            "0",
            "00101",
            "A"
          ),
          (
            ts(0),
            "added",
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
