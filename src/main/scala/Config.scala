import java.net.URL
import java.nio.file.Path


case class Source(
  id: String,
  url: URL,
  compression: Option[String],
  subPath: Option[Path],
  schema: Vector[String] = Vector.empty,
  format: String,
  readerOptions: Map[String, String] = Map.empty
)


case class AppConfig(
  downloadDir: Path,
  dataDir: Path,
  sources: Vector[Source] = Vector.empty
)
