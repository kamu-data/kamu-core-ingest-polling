package dev.kamu.core.ingest.polling.merge

import java.sql.Timestamp

import dev.kamu.core.manifests
import org.apache.spark.sql.DataFrame

object MergeStrategy {
  def apply(kind: manifests.MergeStrategyKind): MergeStrategy = {
    kind match {
      case _: manifests.MergeStrategyAppend =>
        new AppendMergeStrategy()
      case c: manifests.MergeStrategyLedger =>
        new LedgerMergeStrategy(c.primaryKey)
      case c: manifests.MergeStrategySnapshot =>
        new SnapshotMergeStrategy(
          pk = c.primaryKey,
          modInd = c.modificationIndicator
        )
      case _ =>
        throw new NotImplementedError(s"Unsupported strategy: $kind")
    }
  }
}

abstract class MergeStrategy {
  def merge(
    prev: Option[DataFrame],
    curr: DataFrame,
    systemTime: Timestamp
  ): DataFrame
}
