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
import org.apache.spark.sql.functions
import org.scalatest.FunSuite

class IngestGeoJSONTest extends FunSuite with IngestSuite {
  import spark.implicits._

  test("ingest polygons") {
    withTempDir(tempDir => {
      val inputPath = tempDir
        .resolve("src")
        .resolve("polygons.json")

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

      val dataset = yaml.load[DatasetSnapshot](s"""
        |id: dev.kamu.test
        |source:
        |  kind: root
        |  fetch:
        |    kind: url
        |    url: $inputPath
        |    eventTime:
        |      kind: fromSystemTime
        |  read:
        |    kind: geojson
        |  merge:
        |    kind: snapshot
        |    primaryKey:
        |    - id
        |""".stripMargin)

      writeFile(inputPath, inputData)

      val actual = ingest(tempDir, dataset, new Timestamp(0))
        .orderBy("id")

      val expected = sc
        .parallelize(
          Seq(
            (
              new Timestamp(0),
              new Timestamp(0),
              "I",
              "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))",
              "0",
              "00101",
              "A"
            ),
            (
              new Timestamp(0),
              new Timestamp(0),
              "I",
              "POLYGON ((0 0, 20 0, 20 20, 0 20, 0 0))",
              "1",
              "00202",
              "B"
            )
          )
        )
        .toDF(
          "system_time",
          "event_time",
          "observed",
          "geometry",
          "id",
          "zipcode",
          "name"
        )
        .withColumn(
          "geometry",
          functions.callUDF("ST_GeomFromWKT", functions.col("geometry"))
        )

      assertDataFrameEquals(expected, actual, ignoreNullable = true)
    })
  }

}
