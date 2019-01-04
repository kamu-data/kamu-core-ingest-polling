import java.net.URL
import java.nio.file.Path


case class Source(
  id: String,
  url: URL,
  compression: Option[String],
  subPath: Option[Path],
  schemaName: String,
  format: String,
  readerOptions: Map[String, String]
)


case class AppConfig(
  downloadDir: Path,
  dataDir: Path,
  sources: Vector[Source]
)
