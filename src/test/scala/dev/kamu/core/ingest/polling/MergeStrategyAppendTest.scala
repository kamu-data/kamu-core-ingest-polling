/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.core.ingest.polling

import dev.kamu.core.utils.test.KamuDataFrameSuite
import dev.kamu.core.ingest.polling.merge.AppendMergeStrategy
import org.scalatest.FunSuite

class MergeStrategyAppendTest extends FunSuite with KamuDataFrameSuite {
  import spark.implicits._

  protected override val enableHiveSupport = false

  test("From empty") {
    val curr = sc
      .parallelize(
        Seq(
          (ts(0), 1, "a"),
          (ts(0), 2, "b"),
          (ts(0), 3, "c")
        )
      )
      .toDF("eventTime", "id", "data")

    val strategy = new AppendMergeStrategy()

    val actual = strategy
      .merge(None, curr, ts(1), None)
      .orderBy("systemTime", "eventTime", "id")

    val expected = sc
      .parallelize(
        Seq(
          (ts(1), ts(0), 1, "a"),
          (ts(1), ts(0), 2, "b"),
          (ts(1), ts(0), 3, "c")
        )
      )
      .toDF("systemTime", "eventTime", "id", "data")

    assertDataFrameEquals(expected, actual, ignoreNullable = true)
  }

  test("Append existing") {
    val prev = sc
      .parallelize(
        Seq(
          (ts(1), ts(0), 1, "a"),
          (ts(1), ts(0), 2, "b"),
          (ts(1), ts(0), 3, "c")
        )
      )
      .toDF("systemTime", "eventTime", "id", "data")

    val curr = sc
      .parallelize(
        Seq(
          (4, "d"),
          (5, "e"),
          (6, "f")
        )
      )
      .toDF("id", "data")

    val strategy = new AppendMergeStrategy()

    val actual = strategy
      .merge(Some(prev), curr, ts(3), Some(ts(2)))
      .orderBy("systemTime", "eventTime", "id")

    val expected = sc
      .parallelize(
        Seq(
          (ts(3), ts(2), 4, "d"),
          (ts(3), ts(2), 5, "e"),
          (ts(3), ts(2), 6, "f")
        )
      )
      .toDF("systemTime", "eventTime", "id", "data")

    assertDataFrameEquals(expected, actual, ignoreNullable = true)
  }

  test("New column added") {
    val prev = sc
      .parallelize(
        Seq(
          (ts(1), ts(0), 1, "a"),
          (ts(1), ts(0), 2, "b"),
          (ts(1), ts(0), 3, "c")
        )
      )
      .toDF("systemTime", "eventTime", "id", "data")

    val curr = sc
      .parallelize(
        Seq(
          (4, "x", "d"),
          (5, "y", "e"),
          (6, "z", "f")
        )
      )
      .toDF("id", "extra", "data")

    val strategy = new AppendMergeStrategy()

    val actual = strategy
      .merge(Some(prev), curr, ts(3), Some(ts(2)))
      .orderBy("systemTime", "eventTime", "id")

    val expected = sc
      .parallelize(
        Seq(
          (ts(3), ts(2), 4, "d", "x"),
          (ts(3), ts(2), 5, "e", "y"),
          (ts(3), ts(2), 6, "f", "z")
        )
      )
      .toDF("systemTime", "eventTime", "id", "data", "extra")

    assertDataFrameEquals(expected, actual, ignoreNullable = true)
  }

  // TODO: Consider just returning an error, this is a ledger after all
  test("Old column removed") {
    val prev = sc
      .parallelize(
        Seq(
          (ts(1), ts(0), 1, "x", "a"),
          (ts(1), ts(0), 2, "y", "b"),
          (ts(1), ts(0), 3, "z", "c")
        )
      )
      .toDF("systemTime", "eventTime", "id", "extra", "data")

    val curr = sc
      .parallelize(
        Seq(
          (4, "d"),
          (5, "e"),
          (6, "f")
        )
      )
      .toDF("id", "data")

    val strategy = new AppendMergeStrategy()

    val actual = strategy
      .merge(Some(prev), curr, ts(3), Some(ts(2)))
      .orderBy("systemTime", "eventTime", "id")

    val expected = sc
      .parallelize(
        Seq(
          (ts(3), ts(2), 4, null, "d"),
          (ts(3), ts(2), 5, null, "e"),
          (ts(3), ts(2), 6, null, "f")
        )
      )
      .toDF("systemTime", "eventTime", "id", "extra", "data")

    assertDataFrameEquals(expected, actual, ignoreNullable = true)
  }

}
