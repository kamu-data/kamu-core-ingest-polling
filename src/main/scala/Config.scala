import java.net.URL
import java.nio.file.Path


case class ReaderOption(
  name: String,
  value: String
)


case class Source(
  id: String,
  url: URL,
  schemaName: String,
  format: String,
  readerOptions: Vector[ReaderOption]
)


case class AppConfig(
  downloadDir: Path,
  cacheDir: Path,
  dataDir: Path,
  checkpointDir: Path,
  sources: Vector[Source]
)
