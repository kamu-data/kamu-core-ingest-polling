import java.io.PrintWriter
import java.util.zip.{GZIPInputStream, GZIPOutputStream}

import org.apache.hadoop.fs.{FileSystem, Path}
import org.json4s._
import org.json4s.jackson.JsonMethods._

object GeoJSON {

  def toMultiLineJSON(fileSystem: FileSystem, filePath: Path, outPath: Path): Unit = {
    val fileInputStream = fileSystem.open(filePath)
    val gzipInputStream = new GZIPInputStream(fileInputStream)
    val featureCollection = parse(gzipInputStream, true)

    val fileOutputStream = fileSystem.create(outPath)
    val gzipOutputStream = new GZIPOutputStream(fileOutputStream)
    val writer = new PrintWriter(gzipOutputStream)

    val JArray(features) = featureCollection \ "features"
    for (feature <- features) {
      val featureString = compact(render(feature))

      val newObj = JObject("geojson" -> JString(featureString))
        .merge(feature \ "properties")

      writer.write(compact(render(newObj)))
      writer.write("\n")
    }

    writer.close()
  }
}
