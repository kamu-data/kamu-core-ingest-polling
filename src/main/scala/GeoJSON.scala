import java.io.{FileInputStream, FileOutputStream, PrintWriter}
import java.nio.file.Path
import java.util.zip.{GZIPInputStream, GZIPOutputStream}

import org.json4s._
import org.json4s.jackson.JsonMethods._

object GeoJSON {

  def toMultiLineJSON(filePath: Path, outPath: Path): Unit = {
    val fileInputStream = new FileInputStream(filePath.toString)
    val gzipInputStream = new GZIPInputStream(fileInputStream)
    val json = parse(gzipInputStream, true)

    val fileOutputStream = new FileOutputStream(outPath.toString)
    val gzipOutputStream = new GZIPOutputStream(fileOutputStream)
    val writer = new PrintWriter(gzipOutputStream)

    val JArray(features) = json \ "features"
    for (feature <- features) {
      val geomString = compact(render(feature \ "geometry"))

      val newObj = JObject("_geometry" -> JString(geomString))
        .merge(feature \ "properties")

      writer.write(compact(render(newObj)))
      writer.write("\n")
    }

    writer.close()
  }
}
