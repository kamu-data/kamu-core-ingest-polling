import java.util.zip.ZipInputStream

import org.apache.hadoop.fs.{FileSystem, Path}

object FSUtils {

  implicit class PathExt(val p: Path) {
    def resolve(child: String): Path = {
      new Path(p, child)
    }

    def resolve(child: Path): Path = {
      new Path(p, child)
    }
  }


  def extractZipFile(fileSystem: FileSystem, filePath: Path, outputDir: Path): Unit = {
    val inputStream = fileSystem.open(filePath)
    val zipStream = new ZipInputStream(inputStream)

    extractZipFile(fileSystem, zipStream, outputDir)

    zipStream.close()
  }

  def extractZipFile(fileSystem: FileSystem, zipStream: ZipInputStream, outputDir: Path,
                     filterRegex: Option[String] = None): Unit = {
    fileSystem.mkdirs(outputDir)

    Stream
      .continually(zipStream.getNextEntry)
      .takeWhile(_ != null)
      .filter(entry =>
        filterRegex.isEmpty || entry.getName.matches(filterRegex.get))
      .foreach(entry => {
        val outputStream = fileSystem.create(
          outputDir.resolve(entry.getName))

        val buffer = new Array[Byte](1024)

        Stream
          .continually(zipStream.read(buffer))
          .takeWhile(_ != -1)
          .foreach(outputStream.write(buffer, 0, _))

        outputStream.close()
      })
  }

}