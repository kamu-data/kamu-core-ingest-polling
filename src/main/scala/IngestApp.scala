import java.net.URI

import org.apache.log4j.LogManager
import pureconfig.{CamelCase, ConfigFieldMapping}
import pureconfig.generic.ProductHint
import pureconfig.generic.auto._

object IngestApp {
  def main(args: Array[String]) {
    val logger = LogManager.getLogger(getClass.getName)

    implicit def hint[T]: ProductHint[T] =
      ProductHint[T](ConfigFieldMapping(CamelCase, CamelCase))

    val config = prepareConfig(
      pureconfig.loadConfigOrThrow[AppConfig])

    val ingest = new Ingest(config)
    ingest.pollAndIngest()
  }

  def prepareConfig(config: AppConfig): AppConfig = {
    // Fix relative path file URLs
    var conf = config.copy(
      downloadDir = fixRelativeURL(config.downloadDir),
      checkpointDir = fixRelativeURL(config.checkpointDir),
      dataDir = fixRelativeURL(config.dataDir))

    // Apply defaults
    conf = conf.copy(
      sources = conf.sources.map(source =>
        source.copy(
          readerOptions = Source.DEFAULT_READER_OPTIONS ++ source.readerOptions)))

    conf
  }

  def fixRelativeURL(url: URI): URI = {
    URI.create(url.toString.replaceFirst(
      "^file:\\.",
      s"file:${System.getProperty("user.dir")}/."
    ))
  }
}
