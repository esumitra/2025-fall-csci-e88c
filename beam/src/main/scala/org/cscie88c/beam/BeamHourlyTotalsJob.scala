package org.cscie88c.beam

import com.spotify.scio._
import com.spotify.scio.values.SCollection
import com.typesafe.scalalogging.{LazyLogging}
import org.joda.time.{DateTimeZone, Duration, Instant}
import java.time.ZoneId
import java.time.ZoneOffset
import org.apache.beam.sdk.transforms.windowing.IntervalWindow
import org.apache.beam.sdk.options.StreamingOptions
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.values.{PBegin, PCollection}
import org.apache.beam.sdk.io.TextIO
import org.apache.beam.sdk.transforms.Watch
import com.spotify.scio.io.ClosedTap
import com.twitter.algebird.CMS
import com.spotify.scio.io.Tap

object BeamHourlyTotalsJob extends LazyLogging {

  /*
  * run with
beam/target/universal/stage/bin/beam-cms-job \
--input-file=/path-to-input-file \
--output-file=/path-to-output-folder
  */
  def main(cmdLineArgs: Array[String]): Unit = {
    // 1. Create context
    val (sc: ScioContext, args: Args) = ContextAndArgs(cmdLineArgs)
    implicit val scImplicit: ScioContext = sc

    // 2. Read command line arguments

    // 3. Run pipeline
    // uncomment line below to run the beam pipeline
    // runPipeline(inputFile: String, outputFile: String)
  }

  def runPipeline(inputFile: String, outputFile: String)(implicit sc: ScioContext): Unit = ???
  
}
