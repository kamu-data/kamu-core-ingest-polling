/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.core.ingest.polling.convert

import dev.kamu.core.manifests.{ReaderGeojson, ReaderKind}
import org.apache.log4j.LogManager

class ConversionStepFactory {
  val logger = LogManager.getLogger(getClass.getName)

  def getStep(readerConfig: ReaderKind): ConversionStep = {
    readerConfig match {
      case _: ReaderGeojson =>
        logger.info(s"Pre-processing as GeoJSON")
        new GeoJSONConverter()
      case _ =>
        new NoOpConversionStep()
    }
  }

  def getComposedSteps(
    readerConfig: ReaderKind
  ): ConversionStep = {
    getStep(readerConfig)
  }

}
