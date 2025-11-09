package org.cscie88c.beam

import com.spotify.scio._
import com.spotify.scio.values.SCollection
import com.typesafe.scalalogging.{LazyLogging}

/*
 * run with
 beam/target/universal/stage/bin/beam-batch-job \
 --input-file=/path-to-input-file \
 --output-file=/path-to-output-folder
 */
object BeamBatchJob extends LazyLogging {

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