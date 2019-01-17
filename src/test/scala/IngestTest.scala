import java.net.URL
import java.nio.file.Paths
import java.sql.Timestamp

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.FunSuite


class IngestTest extends FunSuite with DataFrameSuiteBase {
  import spark.implicits._

  protected override val enableHiveSupport = false

  test("rfc3339 event time") {
    val raw = sc.parallelize(Seq(
      "2000-01-01T00:00:00-08:00",
      "2000-01-01T00:00:00Z"
    )).toDF("strEventTime")

    val source = Source(
      id = "x",
      url = new URL("http://localhost"),
      format = "csv",
      eventTimeColumn = Some("strEventTime"))

    val ingest = new Ingest(AppConfig(
      Paths.get("."),
      Paths.get("."),
      Vector(source)
    ))

    val df = ingest.ensureEventTime(raw, source)

    val expected = sc.parallelize(Seq(
      new Timestamp(946713600000L),
      new Timestamp(946684800000L)
    )).toDF("eventTime")

    assertDataFrameEquals(expected, df)
  }
}
