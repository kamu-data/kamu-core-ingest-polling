import java.sql.Timestamp

import org.apache.log4j.LogManager
import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.sql.SparkSession


object IngestApp {
  def main(args: Array[String]) {
    val logger = LogManager.getLogger(getClass.getName)
    val config = AppConf.load()
    val ingest = new Ingest(config, hadoopConf, sparkSession, systemTime)
    ingest.pollAndIngest()
  }

  private def sparkConf(): SparkConf = {
    new SparkConf()
      .setAppName("ingest.polling")
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
