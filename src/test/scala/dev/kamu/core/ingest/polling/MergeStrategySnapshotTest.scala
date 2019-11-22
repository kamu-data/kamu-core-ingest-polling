/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.core.ingest.polling

import java.sql.Timestamp

import dev.kamu.core.ingest.polling.merge.SnapshotMergeStrategy
import org.scalatest.FunSuite

class MergeStrategySnapshotTest extends FunSuite with DataFrameSuiteBaseEx {
  import spark.implicits._

  protected override val enableHiveSupport = false

  def ts(milis: Long) = new Timestamp(milis)

  test("PK and Indicator - from empty") {
    val curr = sc
      .parallelize(
        Seq(
          (1, "A", "x", 0),
          (2, "B", "y", 0),
          (3, "C", "z", 0)
        )
      )
      .toDF("id", "name", "data", "version")

    val strategy = new SnapshotMergeStrategy(Vector("id"), Some("version"))

    val actual = strategy
      .merge(None, curr, ts(0))
      .orderBy("systemTime", "id")

    val expected = sc
      .parallelize(
        Seq(
          (ts(0), "I", 1, "A", "x", 0),
          (ts(0), "I", 2, "B", "y", 0),
          (ts(0), "I", 3, "C", "z", 0)
        )
      )
      .toDF("systemTime", "observed", "id", "name", "data", "version")

    assertDataFrameEquals(expected, actual, ignoreNullable = true)
  }

  test("PK and Indicator - no changes") {
    val prev = sc
      .parallelize(
        Seq(
          (ts(0), "I", 1, "A", "x", 0),
          (ts(0), "I", 2, "B", "y", 0),
          (ts(0), "I", 3, "C", "z", 0)
        )
      )
      .toDF("systemTime", "observed", "id", "name", "data", "version")

    val curr = sc
      .parallelize(
        Seq(
          (1, "A", "x", 0),
          (2, "B", "y", 0),
          (3, "C", "z", 0)
        )
      )
      .toDF("id", "name", "data", "version")

    val strategy = new SnapshotMergeStrategy(Vector("id"), Some("version"))

    val actual = strategy.merge(Some(prev), curr, ts(0))

    val expected = sc
      .parallelize(
        Seq.empty[(Timestamp, String, Int, String, String, Int)]
      )
      .toDF("systemTime", "observed", "id", "name", "data", "version")

    assertDataFrameEquals(expected, actual, ignoreNullable = true)
  }

  test("PK Only - all types of changes") {
    val prev = sc
      .parallelize(
        Seq(
          (ts(0), "I", 1, "A", "x"),
          (ts(0), "I", 2, "B", "y"),
          (ts(0), "I", 3, "C", "z")
        )
      )
      .toDF("systemTime", "observed", "id", "name", "data")

    val curr = sc
      .parallelize(
        Seq(
          (2, "B", "y"),
          (3, "C", "zz"),
          (4, "D", ".")
        )
      )
      .toDF("id", "name", "data")

    val strategy = new SnapshotMergeStrategy(Vector("id"), None)

    val actual = strategy
      .merge(Some(prev), curr, ts(0))
      .orderBy("systemTime", "id")

    val expected = sc
      .parallelize(
        Seq(
          (ts(0), "D", 1, "A", "x"),
          (ts(0), "U", 3, "C", "zz"),
          (ts(0), "I", 4, "D", ".")
        )
      )
      .toDF("systemTime", "observed", "id", "name", "data")

    assertDataFrameEquals(expected, actual, ignoreNullable = true)
  }

  test("PK and Indicator - all types of changes") {
    val prev = sc
      .parallelize(
        Seq(
          (ts(0), "I", 1, "A", "x", 0),
          (ts(0), "I", 2, "B", "y", 0),
          (ts(0), "I", 3, "C", "z", 0)
        )
      )
      .toDF("systemTime", "observed", "id", "name", "data", "version")

    val curr = sc
      .parallelize(
        Seq(
          (2, "B", "y", 0),
          (3, "C", "zz", 1),
          (4, "D", ".", 0)
        )
      )
      .toDF("id", "name", "data", "version")

    val strategy = new SnapshotMergeStrategy(Vector("id"), Some("version"))

    val actual = strategy
      .merge(Some(prev), curr, ts(0))
      .orderBy("systemTime", "id")

    val expected = sc
      .parallelize(
        Seq(
          (ts(0), "D", 1, "A", "x", 0),
          (ts(0), "U", 3, "C", "zz", 1),
          (ts(0), "I", 4, "D", ".", 0)
        )
      )
      .toDF("systemTime", "observed", "id", "name", "data", "version")

    assertDataFrameEquals(expected, actual, ignoreNullable = true)
  }

  test("PK and Indicator - new column added") {
    val prev = sc
      .parallelize(
        Seq(
          (ts(0), "I", 1, "A", "x", 0),
          (ts(0), "I", 2, "B", "y", 0),
          (ts(0), "I", 3, "C", "z", 0)
        )
      )
      .toDF("systemTime", "observed", "id", "name", "data", "version")

    val curr = sc
      .parallelize(
        Seq(
          (2, "B", "y", "a", 0),
          (3, "C", "zz", "b", 1),
          (4, "D", ".", "c", 0)
        )
      )
      .toDF("id", "name", "data", "ext", "version")

    val strategy = new SnapshotMergeStrategy(Vector("id"), Some("version"))

    val actual = strategy
      .merge(Some(prev), curr, ts(0))
      .orderBy("systemTime", "id")

    val expected = sc
      .parallelize(
        Seq(
          (ts(0), "D", 1, "A", "x", 0, null),
          (ts(0), "U", 3, "C", "zz", 1, "b"),
          (ts(0), "I", 4, "D", ".", 0, "c")
        )
      )
      .toDF("systemTime", "observed", "id", "name", "data", "version", "ext")

    assertDataFrameEquals(expected, actual, ignoreNullable = true)
  }

  test("PK and Indicator - old column missing") {
    val prev = sc
      .parallelize(
        Seq(
          (ts(0), "I", 1, "A", "x", "a", 0),
          (ts(0), "I", 2, "B", "y", "b", 0),
          (ts(0), "I", 3, "C", "z", "c", 0)
        )
      )
      .toDF("systemTime", "observed", "id", "name", "data", "ext", "version")

    val curr = sc
      .parallelize(
        Seq(
          (2, "B", "y", 1),
          (3, "C", "z", 1),
          (4, "D", ".", 0)
        )
      )
      .toDF("id", "name", "data", "version")

    val strategy = new SnapshotMergeStrategy(Vector("id"), Some("version"))

    val actual = strategy
      .merge(Some(prev), curr, ts(0))
      .orderBy("systemTime", "id")

    val expected = sc
      .parallelize(
        Seq(
          (ts(0), "D", 1, "A", "x", "a", 0),
          (ts(0), "U", 2, "B", "y", null, 1),
          (ts(0), "U", 3, "C", "z", null, 1),
          (ts(0), "I", 4, "D", ".", null, 0)
        )
      )
      .toDF("systemTime", "observed", "id", "name", "data", "ext", "version")

    assertDataFrameEquals(expected, actual, ignoreNullable = true)
  }
}
