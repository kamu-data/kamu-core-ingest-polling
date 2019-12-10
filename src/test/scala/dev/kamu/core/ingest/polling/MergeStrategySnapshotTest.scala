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
import dev.kamu.core.ingest.polling.merge.SnapshotMergeStrategy
import org.scalatest.FunSuite

case class Employee(
  id: Int,
  name: String,
  salary: Int
)

case class EmployeeV2(
  id: Int,
  name: String,
  department: String,
  salary: Int
)

case class EmployeeEvent(
  systemTime: Timestamp,
  eventTime: Timestamp,
  observed: String,
  id: Int,
  name: String,
  salary: Int
)

case class EmployeeEventV2(
  systemTime: Timestamp,
  eventTime: Timestamp,
  observed: String,
  id: Int,
  name: String,
  department: String,
  salary: Int
)

class MergeStrategySnapshotTest extends FunSuite with KamuDataFrameSuite {
  import spark.implicits._

  protected override val enableHiveSupport = false

  test("From empty") {
    val curr = sc
      .parallelize(
        Seq(
          Employee(1, "Alice", 100),
          Employee(2, "Bob", 80),
          Employee(3, "Charlie", 120)
        )
      )
      .toDF()

    val strategy = new SnapshotMergeStrategy(Vector("id"))

    val t_e = new Timestamp(0)
    val t_s = new Timestamp(1)

    val actual = strategy
      .merge(None, curr, t_s, Some(t_e))
      .as[EmployeeEvent]
      .orderBy("systemTime", "eventTime", "id")

    val expected = sc
      .parallelize(
        Seq(
          EmployeeEvent(t_s, t_e, "I", 1, "Alice", 100),
          EmployeeEvent(t_s, t_e, "I", 2, "Bob", 80),
          EmployeeEvent(t_s, t_e, "I", 3, "Charlie", 120)
        )
      )
      .toDS
      .orderBy("systemTime", "eventTime", "id")

    assertDatasetEquals(expected, actual)
  }

  test("No changes") {
    val curr = sc
      .parallelize(
        Seq(
          Employee(1, "Alice", 100),
          Employee(2, "Bob", 80),
          Employee(3, "Charlie", 120)
        )
      )
      .toDF()

    val strategy = new SnapshotMergeStrategy(Vector("id"))

    val t_e1 = new Timestamp(0)
    val t_s1 = new Timestamp(1)

    val prev = strategy.merge(None, curr, t_s1, Some(t_e1))

    val t_e2 = new Timestamp(2)
    val t_s2 = new Timestamp(3)

    val actual = strategy
      .merge(Some(prev), curr, t_s2, Some(t_e2))
      .as[EmployeeEvent]

    assert(actual.isEmpty)
  }

  test("All types of changes") {
    val data1 = sc
      .parallelize(
        Seq(
          Employee(1, "Alice", 100),
          Employee(2, "Bob", 80),
          Employee(3, "Charlie", 120)
        )
      )
      .toDF()

    val data2 = sc
      .parallelize(
        Seq(
          Employee(2, "Bob", 80),
          Employee(3, "Charlie", 130),
          Employee(4, "Dan", 100)
        )
      )
      .toDF()

    val strategy = new SnapshotMergeStrategy(Vector("id"))

    val t_e1 = new Timestamp(0)
    val t_s1 = new Timestamp(1)

    val prev = strategy.merge(None, data1, t_s1, Some(t_e1))

    val t_e2 = new Timestamp(2)
    val t_s2 = new Timestamp(3)

    val actual = strategy
      .merge(Some(prev), data2, t_s2, Some(t_e2))
      .as[EmployeeEvent]
      .orderBy("systemTime", "eventTime", "id")

    val expected = sc
      .parallelize(
        Seq(
          EmployeeEvent(t_s2, t_e2, "D", 1, "Alice", 100),
          EmployeeEvent(t_s2, t_e2, "U", 3, "Charlie", 130),
          EmployeeEvent(t_s2, t_e2, "I", 4, "Dan", 100)
        )
      )
      .toDS()
      .orderBy("systemTime", "eventTime", "id")

    assertDatasetEquals(expected, actual)
  }

  test("All types of changes with duplicates") {
    val data1 = sc
      .parallelize(
        Seq(
          Employee(1, "Alice", 100),
          Employee(2, "Bob", 80),
          Employee(3, "Charlie", 120)
        )
      )
      .toDF()

    val data2 = sc
      .parallelize(
        Seq(
          Employee(2, "Bob", 80),
          Employee(3, "Charlie", 130),
          Employee(3, "Charlie", 130),
          Employee(4, "Dan", 100),
          Employee(4, "Dan", 120)
        )
      )
      .toDF()

    val strategy = new SnapshotMergeStrategy(Vector("id"))

    val t_e1 = new Timestamp(0)
    val t_s1 = new Timestamp(1)

    val prev = strategy.merge(None, data1, t_s1, Some(t_e1))

    val t_e2 = new Timestamp(2)
    val t_s2 = new Timestamp(3)

    val actual = strategy
      .merge(Some(prev), data2, t_s2, Some(t_e2))
      .as[EmployeeEvent]
      .orderBy("systemTime", "eventTime", "id")

    val expected = sc
      .parallelize(
        Seq(
          EmployeeEvent(t_s2, t_e2, "D", 1, "Alice", 100),
          // Complete duplicate removed
          EmployeeEvent(t_s2, t_e2, "U", 3, "Charlie", 130),
          // On PK duplicate currently selects first occurrence (undefined behavior in general)
          EmployeeEvent(t_s2, t_e2, "I", 4, "Dan", 100)
        )
      )
      .toDS()
      .orderBy("systemTime", "eventTime", "id")

    assertDatasetEquals(expected, actual)
  }

  test("Does not support event times from the past") {
    val data1 = sc
      .parallelize(
        Seq(
          Employee(1, "Alice", 100),
          Employee(2, "Bob", 80),
          Employee(3, "Charlie", 120)
        )
      )
      .toDF()

    val data2 = sc
      .parallelize(
        Seq(
          Employee(1, "Alice", 100),
          Employee(2, "Bob", 80),
          Employee(3, "Charlie", 120)
        )
      )
      .toDF()

    val strategy = new SnapshotMergeStrategy(Vector("id"))

    val prev =
      strategy.merge(None, data1, new Timestamp(1), Some(new Timestamp(1)))

    assertThrows[Exception] {
      strategy.merge(
        Some(prev),
        data2,
        new Timestamp(2),
        Some(new Timestamp(0))
      )
    }
  }

  test("New column added") {
    val data1 = sc
      .parallelize(
        Seq(
          Employee(1, "Alice", 100),
          Employee(2, "Bob", 80),
          Employee(3, "Charlie", 120)
        )
      )
      .toDF()

    val data2 = sc
      .parallelize(
        Seq(
          EmployeeV2(2, "Bob", "IT", 80),
          EmployeeV2(3, "Charlie", "IT", 130),
          EmployeeV2(4, "Dan", "Accounting", 100)
        )
      )
      .toDF()

    val strategy = new SnapshotMergeStrategy(Vector("id"))

    val t_e1 = new Timestamp(0)
    val t_s1 = new Timestamp(1)

    val prev = strategy.merge(None, data1, t_s1, Some(t_e1))

    val t_e2 = new Timestamp(2)
    val t_s2 = new Timestamp(3)

    val actual = strategy
      .merge(Some(prev), data2, t_s2, Some(t_e2))
      .as[EmployeeEventV2]
      .orderBy("systemTime", "eventTime", "id")

    val expected = sc
      .parallelize(
        Seq(
          EmployeeEventV2(t_s2, t_e2, "D", 1, "Alice", null, 100),
          EmployeeEventV2(t_s2, t_e2, "U", 2, "Bob", "IT", 80),
          EmployeeEventV2(t_s2, t_e2, "U", 3, "Charlie", "IT", 130),
          EmployeeEventV2(t_s2, t_e2, "I", 4, "Dan", "Accounting", 100)
        )
      )
      .toDS()
      .orderBy("systemTime", "eventTime", "id")

    assertDatasetEquals(expected, actual)
  }

  test("Old column missing") {
    val data1 = sc
      .parallelize(
        Seq(
          EmployeeV2(1, "Alice", "IT", 100),
          EmployeeV2(2, "Bob", "IT", 80),
          EmployeeV2(3, "Charlie", "IT", 120)
        )
      )
      .toDF()

    val data2 = sc
      .parallelize(
        Seq(
          Employee(2, "Bob", 80),
          Employee(3, "Charlie", 120),
          Employee(4, "Dan", 100)
        )
      )
      .toDF()

    val strategy = new SnapshotMergeStrategy(Vector("id"))

    val t_e1 = new Timestamp(0)
    val t_s1 = new Timestamp(1)

    val prev = strategy.merge(None, data1, t_s1, Some(t_e1))

    val t_e2 = new Timestamp(2)
    val t_s2 = new Timestamp(3)

    val actual = strategy
      .merge(Some(prev), data2, t_s2, Some(t_e2))
      .as[EmployeeEventV2]
      .orderBy("systemTime", "eventTime", "id")

    val expected = sc
      .parallelize(
        Seq(
          EmployeeEventV2(t_s2, t_e2, "D", 1, "Alice", "IT", 100),
          EmployeeEventV2(t_s2, t_e2, "U", 2, "Bob", null, 80),
          EmployeeEventV2(t_s2, t_e2, "U", 3, "Charlie", null, 120),
          EmployeeEventV2(t_s2, t_e2, "I", 4, "Dan", null, 100)
        )
      )
      .toDS()
      .orderBy("systemTime", "eventTime", "id")

    assertDatasetEquals(expected, actual)
  }
}
