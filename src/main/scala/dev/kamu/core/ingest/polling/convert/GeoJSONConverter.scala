package dev.kamu.core.ingest.polling.convert

import java.io.{
  ByteArrayInputStream,
  ByteArrayOutputStream,
  InputStream,
  PrintWriter
}

import org.json4s._
import org.json4s.jackson.JsonMethods._

/** Converts single GeoJSON document into a file with individual features separated per line */
class GeoJSONConverter extends ConversionStep {

  // TODO: This is very inefficient, should extend GeoSpark to support this
  def convert(inputStream: InputStream): InputStream = {
    val featureCollection = parse(inputStream, true)
    inputStream.close()

    val outputStream = new ByteArrayOutputStream()
    val writer = new PrintWriter(outputStream)

    val JArray(features) = featureCollection \ "features"
    for (feature <- features) {
      writer.write(compact(render(feature)))
      writer.write("\n")
    }

    writer.close()
    new ByteArrayInputStream(outputStream.toByteArray)
  }
}
