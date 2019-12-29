/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.core.ingest.polling

import dev.kamu.core.manifests._
import dev.kamu.core.manifests.parsing.pureconfig.yaml
import yaml.defaults._
import pureconfig.generic.auto._
import dev.kamu.core.utils.fs._
import org.scalatest.FunSuite

class IngestSnapshotTest extends FunSuite with IngestSuite {
  import spark.implicits._

  test("first time ingest") {
    withTempDir(tempDir => {

      val inputData =
        """1,alex,100,0
          |2,bob,200,0
          |3,charlie,300,0
          |""".stripMargin

      val inputPath = tempDir
        .resolve("src")
        .resolve("balances.csv")

      val dataset = yaml.load[Dataset](s"""
        |id: dev.kamu.test
        |rootPollingSource:
        |  fetch:
        |    kind: fetchUrl
        |    url: $inputPath
        |    eventTime:
        |      kind: fromSystemTime
        |  read:
        |    kind: csv
        |    schema:
        |    - id INT
        |    - name STRING
        |    - balance INT
        |    - version INT
        |  merge:
        |    kind: snapshot
        |    primaryKey:
        |    - id
        |    compareColumns:
        |    - version
        |""".stripMargin)

      writeFile(inputPath, inputData)

      val actual = ingest(tempDir, dataset, ts(0))
        .orderBy("system_time", "id")

      val expected = sc
        .parallelize(
          Seq(
            (ts(0), ts(0), "I", 1, "alex", 100, 0),
            (ts(0), ts(0), "I", 2, "bob", 200, 0),
            (ts(0), ts(0), "I", 3, "charlie", 300, 0)
          )
        )
        .toDF(
          "system_time",
          "event_time",
          "observed",
          "id",
          "name",
          "balance",
          "version"
        )

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

      val inputPath = tempDir
        .resolve("src")
        .resolve("balances.csv")

      val dataset = yaml.load[Dataset](s"""
        |id: dev.kamu.test
        |rootPollingSource:
        |  fetch:
        |    kind: fetchUrl
        |    url: $inputPath
        |    eventTime:
        |      kind: fromSystemTime
        |  read:
        |    kind: csv
        |    schema:
        |    - id INT
        |    - name STRING
        |    - balance INT
        |    - version INT
        |  merge:
        |    kind: snapshot
        |    primaryKey:
        |    - id
        |    compareColumns:
        |    - version
        |""".stripMargin)

      writeFile(inputPath, inputData1)

      ingest(tempDir, dataset, ts(0))

      writeFile(inputPath, inputData2)

      val actual = ingest(tempDir, dataset, ts(1))
        .orderBy("system_time", "event_time", "id")

      val expected = sc
        .parallelize(
          Seq(
            (ts(0), ts(0), "I", 1, "alex", 100, 0),
            (ts(0), ts(0), "I", 2, "bob", 200, 0),
            (ts(0), ts(0), "I", 3, "charlie", 300, 0),
            (ts(1), ts(1), "D", 1, "alex", 100, 0),
            (ts(1), ts(1), "U", 3, "charlie", 500, 1),
            (ts(1), ts(1), "I", 4, "dan", 100, 0)
          )
        )
        .toDF(
          "system_time",
          "event_time",
          "observed",
          "id",
          "name",
          "balance",
          "version"
        )

      assertDataFrameEquals(expected, actual, ignoreNullable = true)
    })
  }

}
