import pureconfig.{CamelCase, ConfigFieldMapping}
import pureconfig.generic.ProductHint
import pureconfig.generic.auto._

object IngestApp {
  def main(args: Array[String]) {
    implicit def hint[T]: ProductHint[T] =
      ProductHint[T](ConfigFieldMapping(CamelCase, CamelCase))

    val config = pureconfig.loadConfigOrThrow[AppConfig]

    val configWithDefaults = config.copy(
      sources = config.sources.map(source => source.copy(
        readerOptions = Source.DEFAULT_READER_OPTIONS ++ source.readerOptions)))

    val ingest = new Ingest(configWithDefaults)
    ingest.pollAndIngest()
  }
}
