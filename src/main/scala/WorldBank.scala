import java.io.{FileInputStream, FileOutputStream, PrintWriter}
import java.nio.file.Path
import java.util.zip.{GZIPInputStream, GZIPOutputStream}

// Removes the poorly formatted header
// Removes trailing commas from all lines
object WorldBank {

  val HEADER_NUM_LINES = 4

  def toPlainCSV(filePath: Path, outPath: Path): Unit = {
    val fileInputStream = new FileInputStream(filePath.toString)
    val gzipInputStream = new GZIPInputStream(fileInputStream)
    val source = scala.io.Source.fromInputStream(gzipInputStream)

    val fileOutputStream = new FileOutputStream(outPath.toString)
    val gzipOutputStream = new GZIPOutputStream(fileOutputStream)
    val writer = new PrintWriter(gzipOutputStream)

    for (line <- source.getLines.drop(HEADER_NUM_LINES)) {
      writer.write(line.replaceAll(",$", ""))
      writer.write("\n")
    }

    writer.close()
  }
}
