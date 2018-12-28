import pureconfig.{CamelCase, ConfigFieldMapping}
import pureconfig.generic.ProductHint
import pureconfig.generic.auto._

object IngestApp {
  def main(args: Array[String]) {
    implicit def hint[T]: ProductHint[T] =
      ProductHint[T](ConfigFieldMapping(CamelCase, CamelCase))

    val config = pureconfig.loadConfigOrThrow[AppConfig]

    val ingest = new Ingest(config)
    ingest.pollAndIngest()
  }
}
