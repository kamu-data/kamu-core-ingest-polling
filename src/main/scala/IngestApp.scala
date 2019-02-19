import org.apache.log4j.LogManager


object IngestApp {
  def main(args: Array[String]) {
    val logger = LogManager.getLogger(getClass.getName)
    val config = AppConfig.load()
    val ingest = new Ingest(config)
    ingest.pollAndIngest()
  }


}
