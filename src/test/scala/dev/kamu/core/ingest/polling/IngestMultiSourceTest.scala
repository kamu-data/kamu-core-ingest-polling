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

class IngestMultiSourceTest extends FunSuite with IngestSuite {
  import spark.implicits._

  test("files glob") {
    withTempDir(tempDir => {
      val inputData1 =
        """1,alex,100
        |2,bob,200
        |3,charlie,300
        |""".stripMargin

      val inputData2 =
        """2,bob,200
        |3,charlie,500
        |4,dan,100
        |""".stripMargin

      val inputPath1 = tempDir
        .resolve("src")
        .resolve("balances-2001-01-01.csv")

      val inputPath2 = tempDir
        .resolve("src")
        .resolve("balances-2001-02-01.csv")

      writeFile(inputPath1, inputData1)
      writeFile(inputPath2, inputData2)

      val dataset = yaml.load[DatasetSnapshot](s"""
          |id: dev.kamu.test
          |rootPollingSource:
          |  fetch:
          |    kind: fetchFilesGlob
          |    path: ${tempDir.resolve("src").resolve("balances-*.csv")}
          |    eventTime:
          |      kind: fromPath
          |      pattern: balances-(\\d+-\\d+-\\d+)\\.csv
          |      timestampFormat: yyyy-MM-dd
          |    cache:
          |      kind: forever
          |  read:
          |    kind: csv
          |    schema:
          |    - id INT
          |    - name STRING
          |    - balance INT
          |  preprocess:
          |  - kind: sparkSQL
          |    query: SELECT id, name, balance FROM input
          |  merge:
          |    kind: snapshot
          |    primaryKey:
          |    - id
          |""".stripMargin)

      // Should ingest both files
      ingest(tempDir, dataset, ts(0))

      // "touch" files
      fileSystem.setTimes(inputPath1, System.currentTimeMillis(), -1)
      fileSystem.setTimes(inputPath2, System.currentTimeMillis(), -1)

      // Second ingest should ignore modified files as we cache forever
      val actual = ingest(tempDir, dataset, ts(0))
        .orderBy("system_time", "event_time", "id")

      val expected = sc
        .parallelize(
          Seq(
            (ts(0), ts(2001, 1, 1), "I", 1, "alex", 100),
            (ts(0), ts(2001, 1, 1), "I", 2, "bob", 200),
            (ts(0), ts(2001, 1, 1), "I", 3, "charlie", 300),
            (ts(0), ts(2001, 2, 1), "D", 1, "alex", 100),
            (ts(0), ts(2001, 2, 1), "U", 3, "charlie", 500),
            (ts(0), ts(2001, 2, 1), "I", 4, "dan", 100)
          )
        )
        .toDF("system_time", "event_time", "observed", "id", "name", "balance")

      assertDataFrameEquals(expected, actual, ignoreNullable = true)
    })
  }

}
