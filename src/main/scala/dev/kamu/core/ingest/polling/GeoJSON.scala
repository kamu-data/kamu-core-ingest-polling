package dev.kamu.core.ingest.polling

import java.io.{InputStream, OutputStream, PrintWriter}

import org.json4s._
import org.json4s.jackson.JsonMethods._

object GeoJSON {

  // TODO: This is very inefficient, should extend GeoSpark to support this
  def toMultiLineJSON(inputStream: InputStream,
                      outputStream: OutputStream): Unit = {
    val featureCollection = parse(inputStream, true)
    val writer = new PrintWriter(outputStream)

    val JArray(features) = featureCollection \ "features"
    for (feature <- features) {
      writer.write(compact(render(feature)))
      writer.write("\n")
    }

    inputStream.close()
    writer.close()
  }
}
