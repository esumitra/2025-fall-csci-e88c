package org.cscie88c.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import java.io.File

object GoldValidationJob {

  val GoldRoot = "/opt/spark-data/gold/kpis"
  val ValidationOut = "/opt/spark-data/gold/validation"

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Gold-Validation-Job")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    // ================================================================
    // Step 1 — Locate two latest KPI runs
    // ================================================================
    val rootDir = new File(GoldRoot)
    if (!rootDir.exists() || !rootDir.isDirectory)
      throw new RuntimeException(s"Gold KPI root does not exist: $GoldRoot")

    val runDirs = rootDir
      .listFiles()
      .filter(f => f.isDirectory && f.getName.startsWith("run_"))
      .sortBy(_.getName)

    if (runDirs.length < 2)
      throw new RuntimeException("Need at least two KPI runs for validation.")

    val prevRun = runDirs(runDirs.length - 2).getAbsolutePath
    val currRun = runDirs(runDirs.length - 1).getAbsolutePath

    println(s"✅ Using CURRENT run:  $currRun")
    println(s"✅ Using PREVIOUS run: $prevRun")

    // ================================================================
    // Step 2 — Load the weekly_trip_volume metric
    // ================================================================
    val currentVol = spark.read
      .parquet(s"$currRun/weekly_trip_volume")
      .withColumnRenamed("trip_volume", "trip_volume_curr")

    val previousVol = spark.read
      .parquet(s"$prevRun/weekly_trip_volume")
      .withColumnRenamed("trip_volume", "trip_volume_prev")

    // ================================================================
    // Step 3 — Join and compute changes
    // ================================================================
    val joined = currentVol
      .join(
        previousVol,
        Seq("pickup_week_start", "pickup_borough"),
        "inner"
      )
      .withColumn(
        "change_pct",
        (col("trip_volume_curr") - col("trip_volume_prev")) / col(
          "trip_volume_prev"
        )
      )

    // Flag anomalies (25% increase or decrease)
    val anomalies = joined.filter(abs(col("change_pct")) > 0.25)

    // ================================================================
    // Step 4 — Write validation output
    // ================================================================
    val outDir = s"$ValidationOut/run_${System.currentTimeMillis()}"

    anomalies
      .coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv(outDir)

    println(s"✅ Validation complete. Output written to: $outDir")

    spark.stop()
  }
}
