/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.core.ingest.polling

import java.sql.Timestamp

import dev.kamu.core.ingest.polling.merge.AppendMergeStrategy
import org.scalatest.FunSuite

class MergeStrategyAppendTest extends FunSuite with DataFrameSuiteBaseEx {
  import spark.implicits._

  protected override val enableHiveSupport = false

  def ts(milis: Long) = new Timestamp(milis)

  test("From empty") {
    val curr = sc
      .parallelize(
        Seq(
          (1, "a"),
          (2, "b"),
          (3, "c")
        )
      )
      .toDF("id", "data")

    val strategy = new AppendMergeStrategy()

    val actual = strategy
      .merge(None, curr, ts(1))
      .orderBy("systemTime", "id")

    val expected = sc
      .parallelize(
        Seq(
          (ts(1), 1, "a"),
          (ts(1), 2, "b"),
          (ts(1), 3, "c")
        )
      )
      .toDF("systemTime", "id", "data")

    assertDataFrameEquals(expected, actual, ignoreNullable = true)
  }

  test("Append existing") {
    val prev = sc
      .parallelize(
        Seq(
          (ts(1), 1, "a"),
          (ts(1), 2, "b"),
          (ts(1), 3, "c")
        )
      )
      .toDF("systemTime", "id", "data")

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
      .merge(Some(prev), curr, ts(2))
      .orderBy("systemTime", "id")

    val expected = sc
      .parallelize(
        Seq(
          (ts(2), 4, "d"),
          (ts(2), 5, "e"),
          (ts(2), 6, "f")
        )
      )
      .toDF("systemTime", "id", "data")

    assertDataFrameEquals(expected, actual, ignoreNullable = true)
  }

  test("New column added") {
    val prev = sc
      .parallelize(
        Seq(
          (ts(1), 1, "a"),
          (ts(1), 2, "b"),
          (ts(1), 3, "c")
        )
      )
      .toDF("systemTime", "id", "data")

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
      .merge(Some(prev), curr, ts(2))
      .orderBy("systemTime", "id")

    val expected = sc
      .parallelize(
        Seq(
          (ts(2), 4, "d", "x"),
          (ts(2), 5, "e", "y"),
          (ts(2), 6, "f", "z")
        )
      )
      .toDF("systemTime", "id", "data", "extra")

    assertDataFrameEquals(expected, actual, ignoreNullable = true)
  }

  // TODO: Consider just returning an error, this is a ledger after all
  test("Old column removed") {
    val prev = sc
      .parallelize(
        Seq(
          (ts(1), 1, "x", "a"),
          (ts(1), 2, "y", "b"),
          (ts(1), 3, "z", "c")
        )
      )
      .toDF("systemTime", "id", "extra", "data")

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
      .merge(Some(prev), curr, ts(2))
      .orderBy("systemTime", "id")

    val expected = sc
      .parallelize(
        Seq(
          (ts(2), 4, null, "d"),
          (ts(2), 5, null, "e"),
          (ts(2), 6, null, "f")
        )
      )
      .toDF("systemTime", "id", "extra", "data")

    assertDataFrameEquals(expected, actual, ignoreNullable = true)
  }

}
