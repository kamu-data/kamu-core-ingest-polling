package dev.kamu.core.ingest.polling

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.charset.StandardCharsets

import dev.kamu.core.ingest.polling.prep.ProcessPipeStep
import org.apache.commons.io.IOUtils
import org.scalatest.FunSuite

class ProcessPipeStepTest extends FunSuite {
  test("pipe succeeds") {
    val input = new ByteArrayInputStream(
      "this is a test".getBytes(StandardCharsets.UTF_8)
    )
    val outputBuffer = new ByteArrayOutputStream(1024)

    val step = new ProcessPipeStep(Vector("wc", "-w"))
    val output = step.prepare(input)

    IOUtils.copy(output, outputBuffer)
    step.join()

    val actual = new String(outputBuffer.toByteArray, StandardCharsets.UTF_8)
    assert(actual.trim == "4")
  }

  test("pipe fails") {
    val input = new ByteArrayInputStream(
      "this is a test".getBytes(StandardCharsets.UTF_8)
    )
    val outputBuffer = new ByteArrayOutputStream(1024)

    val step = new ProcessPipeStep(Vector("false"))
    val output = step.prepare(input)

    IOUtils.copy(output, outputBuffer)

    assertThrows[RuntimeException] {
      step.join()
    }

    val actual = new String(outputBuffer.toByteArray, StandardCharsets.UTF_8)
    assert(actual.trim == "")
  }

}
