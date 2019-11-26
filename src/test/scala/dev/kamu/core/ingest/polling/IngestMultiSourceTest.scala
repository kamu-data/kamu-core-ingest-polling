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
import dev.kamu.core.manifests.parsing.pureconfig.yaml
import yaml.defaults._
import pureconfig.generic.auto._
import dev.kamu.core.utils.fs._
import org.scalatest.FunSuite

class IngestMultiSourceTest extends FunSuite with IngestSuite {
  import spark.implicits._

  def ts(milis: Long) = new Timestamp(milis)

  ignore("files glob") {
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

      val inputSchema =
        Vector("id INT", "name STRING", "balance INT")

      val inputPath1 = tempDir
        .resolve("src")
        .resolve("balances-2001-01-01.csv")

      val inputPath2 = tempDir
        .resolve("src")
        .resolve("balances-2001-02-01.csv")

      writeFile(inputPath1, inputData1)
      writeFile(inputPath2, inputData2)

      val dataset = yaml.load[Dataset](s"""
          |id: dev.kamu.test
          |rootPollingSource:
          |  fetch:
          |    kind: fetchFilesGlob
          |    path: ${tempDir.resolve("src").resolve("balances-*.csv")}
          |    cache:
          |      kind: forever
          |  read:
          |    kind: csv
          |    schema:
          |    - id INT
          |    - name STRING
          |    - balance INT
          |  preprocess:
          |  - view: output
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
        .orderBy("id")

      actual.show()

      val expected = sc
        .parallelize(
          Seq(
            (ts(0), "I", "2001-01-01", 1, "alex", 100),
            (ts(0), "I", "2001-01-01", 2, "bob", 200),
            (ts(0), "I", "2001-01-01", 3, "charlie", 300),
            (ts(0), "D", "2001-02-01", 1, "alex", 100),
            (ts(0), "U", "2001-02-01", 3, "charlie", 500),
            (ts(0), "I", "2001-02-01", 4, "dan", 100)
          )
        )
        .toDF("systemTime", "observed", "eventTime", "id", "name", "balance")

      assertDataFrameEquals(expected, actual, ignoreNullable = true)
    })
  }

}
