/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.core.ingest.polling

import java.sql.Timestamp

import dev.kamu.core.manifests._
import dev.kamu.core.utils.fs._
import org.scalatest.FunSuite

class IngestSnapshotTest extends FunSuite with IngestSuite {
  import spark.implicits._

  def ts(milis: Long) = new Timestamp(milis)

  test("first time ingest") {
    withTempDir(tempDir => {

      val inputData =
        """1,alex,100,0
          |2,bob,200,0
          |3,charlie,300,0
          |""".stripMargin

      val inputSchema =
        Vector("id INT", "name STRING", "balance INT", "version INT")

      val inputPath = tempDir
        .resolve("src")
        .resolve("balances.csv")

      val dataset = Dataset(
        id = DatasetID("dev.kamu.test"),
        rootPollingSource = Some(
          RootPollingSource(
            fetch = ExternalSourceKind.FetchUrl(inputPath.toUri),
            read = ReaderKind.Csv(schema = inputSchema),
            merge = MergeStrategyKind.Snapshot(
              primaryKey = Vector("id"),
              compareColumns = Vector("version")
            )
          )
        )
      ).postLoad()

      writeFile(inputPath, inputData)

      val actual = ingest(tempDir, dataset, ts(0))
        .orderBy("systemTime", "id")

      val expected = sc
        .parallelize(
          Seq(
            (ts(0), "I", 1, "alex", 100, 0),
            (ts(0), "I", 2, "bob", 200, 0),
            (ts(0), "I", 3, "charlie", 300, 0)
          )
        )
        .toDF("systemTime", "observed", "id", "name", "balance", "version")

      assertDataFrameEquals(expected, actual, ignoreNullable = true)
    })
  }

  test("merge with existing") {
    withTempDir(tempDir => {
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

      val inputPath = tempDir
        .resolve("src")
        .resolve("balances.csv")

      val dataset = Dataset(
        id = DatasetID("dev.kamu.test"),
        rootPollingSource = Some(
          RootPollingSource(
            fetch = ExternalSourceKind.FetchUrl(inputPath.toUri),
            read = ReaderKind.Csv(schema = inputSchema),
            merge = MergeStrategyKind.Snapshot(
              primaryKey = Vector("id"),
              compareColumns = Vector("version")
            )
          )
        )
      ).postLoad()

      writeFile(inputPath, inputData1)

      ingest(tempDir, dataset, ts(0))

      writeFile(inputPath, inputData2)

      val actual = ingest(tempDir, dataset, ts(1))
        .orderBy("systemTime", "id")

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

      assertDataFrameEquals(expected, actual, ignoreNullable = true)
    })
  }

}
