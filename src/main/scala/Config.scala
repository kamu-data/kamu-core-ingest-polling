import java.net.URL
import java.nio.file.Path


object Source {
  val DEFAULT_READER_OPTIONS: Map[String, String] = Map(
    "mode" -> "FAILFAST"
  )
}


case class Source(
  id: String,
  url: URL,
  compression: Option[String],
  subPath: Option[Path],
  schema: Vector[String] = Vector.empty,
  format: String,
  readerOptions: Map[String, String] = Map.empty  // Merged with DEFAULT_READER_OPTIONS
)


case class AppConfig(
  downloadDir: Path,
  dataDir: Path,
  sources: Vector[Source] = Vector.empty
)
