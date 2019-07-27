package dev.kamu.core.ingest.polling

import java.sql.Timestamp

import org.apache.log4j.LogManager
import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator

object IngestApp {
  def main(args: Array[String]) {
    val logger = LogManager.getLogger(getClass.getName)
    val config = AppConf.load()
    if (config.datasets.isEmpty) {
      logger.warn("No data sources specified")
    } else {
      val ingest = new Ingest(config, hadoopConf, sparkSession, systemTime)
      ingest.pollAndIngest()
    }
  }

  private def sparkConf(): SparkConf = {
    new SparkConf()
      .setAppName("ingest.polling")
      .set("spark.serializer", classOf[KryoSerializer].getName)
      .set("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
  }

  private def hadoopConf(): org.apache.hadoop.conf.Configuration = {
    SparkHadoopUtil.get.newConfiguration(sparkConf)
  }

  private def sparkSession(): SparkSession = {
    SparkSession.builder
      .config(sparkConf)
      .getOrCreate()
  }

  private def systemTime(): Timestamp = {
    new Timestamp(System.currentTimeMillis())
  }
}
