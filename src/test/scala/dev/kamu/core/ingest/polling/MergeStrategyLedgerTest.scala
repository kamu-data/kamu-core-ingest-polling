/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.core.ingest.polling

import java.sql.Timestamp

import dev.kamu.core.utils.test.KamuDataFrameSuite
import dev.kamu.core.ingest.polling.merge.LedgerMergeStrategy
import dev.kamu.core.utils.ManualClock
import org.scalatest.FunSuite

class MergeStrategyLedgerTest extends FunSuite with KamuDataFrameSuite {
  import spark.implicits._

  protected override val enableHiveSupport = false

  def clockAt(timestamp: Timestamp) = {
    val systemClock = new ManualClock()
    systemClock.set(timestamp)
    systemClock
  }

  test("From empty") {
    val curr = sc
      .parallelize(
        Seq(
          (ts(0), 1, "a"),
          (ts(1), 2, "b"),
          (ts(2), 3, "c")
        )
      )
      .toDF("event_time", "id", "data")

    val strategy = new LedgerMergeStrategy(Vector("id"), clockAt(ts(3)))

    val actual = strategy
      .merge(None, curr)
      .orderBy("system_time", "event_time", "id")

    val expected = sc
      .parallelize(
        Seq(
          (ts(3), ts(0), 1, "a"),
          (ts(3), ts(1), 2, "b"),
          (ts(3), ts(2), 3, "c")
        )
      )
      .toDF("system_time", "event_time", "id", "data")

    assertDataFrameEquals(expected, actual, ignoreNullable = true)
  }

  test("Append existing") {
    val prev = sc
      .parallelize(
        Seq(
          (ts(3), ts(0), 1, "a"),
          (ts(3), ts(1), 2, "b"),
          (ts(3), ts(2), 3, "c")
        )
      )
      .toDF("system_time", "event_time", "id", "data")

    val curr = sc
      .parallelize(
        Seq(
          (ts(0), 1, "a"),
          (ts(1), 2, "b"),
          (ts(2), 3, "c"),
          (ts(3), 4, "d"),
          (ts(4), 5, "e"),
          (ts(5), 6, "f")
        )
      )
      .toDF("event_time", "id", "data")

    val strategy = new LedgerMergeStrategy(Vector("id"), clockAt(ts(6)))

    val actual = strategy
      .merge(Some(prev), curr)
      .orderBy("system_time", "event_time", "id")

    val expected = sc
      .parallelize(
        Seq(
          (ts(6), ts(3), 4, "d"),
          (ts(6), ts(4), 5, "e"),
          (ts(6), ts(5), 6, "f")
        )
      )
      .toDF("system_time", "event_time", "id", "data")

    assertDataFrameEquals(expected, actual, ignoreNullable = true)
  }

  test("New column added") {
    val prev = sc
      .parallelize(
        Seq(
          (ts(3), ts(0), 1, "a"),
          (ts(3), ts(1), 2, "b"),
          (ts(3), ts(2), 3, "c")
        )
      )
      .toDF("system_time", "event_time", "id", "data")

    val curr = sc
      .parallelize(
        Seq(
          (ts(0), 1, "a", "?"),
          (ts(1), 2, "b", "?"),
          (ts(2), 3, "c", "?"),
          (ts(3), 4, "x", "d"),
          (ts(4), 5, "y", "e"),
          (ts(5), 6, "z", "f")
        )
      )
      .toDF("event_time", "id", "extra", "data")

    val strategy = new LedgerMergeStrategy(Vector("id"), clockAt(ts(6)))

    val actual = strategy
      .merge(Some(prev), curr)
      .orderBy("system_time", "event_time", "id")

    val expected = sc
      .parallelize(
        Seq(
          (ts(6), ts(3), 4, "d", "x"),
          (ts(6), ts(4), 5, "e", "y"),
          (ts(6), ts(5), 6, "f", "z")
        )
      )
      .toDF("system_time", "event_time", "id", "data", "extra")

    assertDataFrameEquals(expected, actual, ignoreNullable = true)
  }

  // TODO: Consider just returning an error, this is a ledger after all
  test("Old column removed") {
    val prev = sc
      .parallelize(
        Seq(
          (ts(3), ts(0), 1, "x", "a"),
          (ts(3), ts(1), 2, "y", "b"),
          (ts(3), ts(2), 3, "z", "c")
        )
      )
      .toDF("system_time", "event_time", "id", "extra", "data")

    val curr = sc
      .parallelize(
        Seq(
          (ts(0), 1, "a"),
          (ts(1), 2, "b"),
          (ts(2), 3, "c"),
          (ts(3), 4, "d"),
          (ts(4), 5, "e"),
          (ts(5), 6, "f")
        )
      )
      .toDF("event_time", "id", "data")

    val strategy = new LedgerMergeStrategy(Vector("id"), clockAt(ts(6)))

    val actual = strategy
      .merge(Some(prev), curr)
      .orderBy("system_time", "event_time", "id")

    val expected = sc
      .parallelize(
        Seq(
          (ts(6), ts(3), 4, null, "d"),
          (ts(6), ts(4), 5, null, "e"),
          (ts(6), ts(5), 6, null, "f")
        )
      )
      .toDF("system_time", "event_time", "id", "extra", "data")

    assertDataFrameEquals(expected, actual, ignoreNullable = true)
  }

}
