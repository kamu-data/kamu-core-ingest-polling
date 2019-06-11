package dev.kamu.core.ingest.polling

import java.io.PrintWriter
import java.sql.Timestamp
import java.util.UUID

import dev.kamu.core.manifests.utils.fs._
import dev.kamu.core.manifests.{
  DataSourcePolling,
  RepositoryVolumeMap,
  Snapshot
}
import org.apache.hadoop
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.scalatest.FunSuite

class IngestSnapshotTest extends FunSuite with DataFrameSuiteBaseEx {
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
    val sourceID = "com.kamu.test"

    val inputPath = tempDir
      .resolve("src")
      .resolve(UUID.randomUUID.toString + ".csv")

    writeFile(inputPath, inputData)

    val conf = AppConf(
      repository = RepositoryVolumeMap(
        downloadDir = tempDir.resolve("poll"),
        checkpointDir = tempDir.resolve("checkpoint"),
        dataDirRoot = tempDir.resolve("root"),
        dataDirDeriv = tempDir.resolve("deriv")
      ),
      sources = List(
        DataSourcePolling(
          id = sourceID,
          url = inputPath.toUri,
          format = "csv",
          schema = inputSchema,
          mergeStrategy =
            Snapshot(primaryKey = "id", modificationIndicator = Some("version"))
        ).postLoad()
      )
    )

    val ingest = new Ingest(
      config = conf,
      hadoopConf = new hadoop.conf.Configuration(),
      getSparkSession = () => spark,
      getSystemTime = () => systemTime
    )

    ingest.pollAndIngest()

    val outputDir = conf.repository.dataDirRoot.resolve(sourceID)

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
          (ts(0), "added", 1, "alex", 100, 0),
          (ts(0), "added", 2, "bob", 200, 0),
          (ts(0), "added", 3, "charlie", 300, 0)
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
          (ts(0), "added", 1, "alex", 100, 0),
          (ts(0), "added", 2, "bob", 200, 0),
          (ts(0), "added", 3, "charlie", 300, 0),
          (ts(1), "removed", 1, "alex", 100, 0),
          (ts(1), "changed", 3, "charlie", 500, 1),
          (ts(1), "added", 4, "dan", 100, 0)
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
