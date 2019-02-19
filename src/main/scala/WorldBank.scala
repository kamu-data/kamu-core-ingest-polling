import java.io.PrintWriter
import java.util.zip.{GZIPInputStream, GZIPOutputStream}

import org.apache.hadoop.fs.{FileSystem, Path}

/** Removes the poorly formatted header
  * Removes trailing commas from all lines
  */
object WorldBank {

  val HEADER_NUM_LINES = 4

  def toPlainCSV(fileSystem: FileSystem, filePath: Path, outPath: Path): Unit = {
    val fileInputStream = fileSystem.open(filePath)
    val gzipInputStream = new GZIPInputStream(fileInputStream)
    val source = scala.io.Source.fromInputStream(gzipInputStream)

    val fileOutputStream = fileSystem.create(outPath)
    val gzipOutputStream = new GZIPOutputStream(fileOutputStream)
    val writer = new PrintWriter(gzipOutputStream)

    for (line <- source.getLines.drop(HEADER_NUM_LINES)) {
      writer.write(line.replaceAll(",$", ""))
      writer.write("\n")
    }

    writer.close()
  }
}
