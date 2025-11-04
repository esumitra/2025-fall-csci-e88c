package org.cscie88c.spark

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import java.io.File
import java.nio.file.{Files, Paths, StandardCopyOption}

object SilverJob {

  val TripsParquetPath  = "/opt/spark-data/yellow_tripdata_2025-01.parquet"
  val TaxiZonesCsvPath  = "/opt/spark-data/taxi_zone_lookup.csv"

  val SilverRoot        = "/opt/spark-data/silver"
  val SilverTripsOut    = s"$SilverRoot/trips"

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("SilverTripCleaning-MultiRule-SingleParquet")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    // Step 1: Load source data
    val tripsDF = spark.read.parquet(TripsParquetPath)
    println(s"=== DEBUG: Raw input count === ${tripsDF.count()}")

    // Step 2: Derived features
    val withDerived = tripsDF
      .withColumn(
        "trip_duration_min",
        (col("tpep_dropoff_datetime").cast("long") - col("tpep_pickup_datetime").cast("long")) / 60.0
      )
      .withColumn("pickup_date", to_date(col("tpep_pickup_datetime")))
      .withColumn("pickup_hour", hour(col("tpep_pickup_datetime")))

    println("=== DEBUG: Sample trip_duration_min values ===")
    withDerived.select(
      col("tpep_pickup_datetime"),
      col("tpep_dropoff_datetime"),
      col("trip_duration_min")
    ).show(10, truncate = false)

    var currentDF = withDerived
    val totalRows = currentDF.count()

    // Helper: apply rule, print metrics, write rejects
    def applyDQRule(df: DataFrame, ruleName: String, condition: org.apache.spark.sql.Column): DataFrame = {
      println(s"\n=== APPLYING RULE: $ruleName ===")

      val rejects = df.filter(!condition)
      val cleaned = df.filter(condition)

      val totalBefore   = df.count()
      val rejectedCount = rejects.count()
      val keptCount     = cleaned.count()

      println(s"=== METRICS: $ruleName ===")
      println(f"Total before:  $totalBefore%,d")
      println(f"Kept rows:     $keptCount%,d")
      println(f"Rejected rows: $rejectedCount%,d")
      println(f"Check sum:     ${keptCount + rejectedCount}%,d (should match total before)")

      val rejectsCsvOut = s"$SilverRoot/rejects_${ruleName}"
      rejects.coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .option("quoteAll", "true")
        .csv(rejectsCsvOut)
      println(s"Rejects written to: $rejectsCsvOut")

      cleaned
    }

    // ✅ Step 3: Remove nulls upfront
    println("\n=== APPLYING INITIAL RULE: no_null_values ===")
    val rejectsNulls = currentDF.filter(currentDF.columns.map(c => col(c).isNull).reduce(_ || _))
    val cleanedNoNulls = currentDF.filter(currentDF.columns.map(c => col(c).isNotNull).reduce(_ && _))

    val totalBeforeNulls = currentDF.count()
    val rejectedNulls = rejectsNulls.count()
    val keptNoNulls = cleanedNoNulls.count()

    println("=== METRICS: no_null_values ===")
    println(f"Total before:  $totalBeforeNulls%,d")
    println(f"Kept rows:     $keptNoNulls%,d")
    println(f"Rejected rows: $rejectedNulls%,d")
    println(f"Check sum:     ${keptNoNulls + rejectedNulls}%,d (should match total before)")

    val rejectsCsvOutInit = s"$SilverRoot/rejects_no_null_values"
    rejectsNulls.coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .option("quoteAll", "true")
      .csv(rejectsCsvOutInit)
    println(s"Rejects written to: $rejectsCsvOutInit")

    currentDF = cleanedNoNulls

    // Step 4: Sequential DQ rules with metrics + CSV rejects
    println("\n=== STARTING DQ CLEANING PIPELINE ===")

    currentDF = applyDQRule(currentDF, "passenger_count_valid", col("passenger_count").between(1, 8))
    currentDF = applyDQRule(currentDF, "total_amount_nonnegative", col("total_amount") >= 0)
    currentDF = applyDQRule(currentDF, "chronological_order_valid", col("tpep_dropoff_datetime") >= col("tpep_pickup_datetime"))
    currentDF = applyDQRule(currentDF, "duration_reasonable", col("trip_duration_min").between(1.0, 180.0))
    currentDF = applyDQRule(currentDF, "distance_reasonable", col("trip_distance").between(0.1, 100.0))
    currentDF = applyDQRule(currentDF, "payment_type_valid", col("payment_type").between(1, 6))
    currentDF = applyDQRule(currentDF, "ratecode_valid", col("RatecodeID").between(1, 6))
    currentDF = applyDQRule(currentDF, "pickup_dropoff_not_null",
      col("tpep_pickup_datetime").isNotNull && col("tpep_dropoff_datetime").isNotNull
    )

    // Step 5: Remove negative numeric values
    println("\n=== APPLYING RULE: no_negative_values ===")
    val numericCols = currentDF.schema.fields.filter(f =>
      f.dataType.simpleString.matches("int|double|float|long|decimal.*")
    ).map(_.name)

    if (numericCols.nonEmpty) {
      val negativeCondition = numericCols.map(c => col(c) >= 0).reduce(_ && _)
      val rejectsNeg = currentDF.filter(!negativeCondition)
      val cleanedNeg = currentDF.filter(negativeCondition)

      val totalBeforeNeg = currentDF.count()
      val rejectedNeg = rejectsNeg.count()
      val keptNeg = cleanedNeg.count()

      println("=== METRICS: no_negative_values ===")
      println(f"Total before:  $totalBeforeNeg%,d")
      println(f"Kept rows:     $keptNeg%,d")
      println(f"Rejected rows: $rejectedNeg%,d")
      println(f"Check sum:     ${keptNeg + rejectedNeg}%,d (should match total before)")

      val rejectsCsvOut = s"$SilverRoot/rejects_no_negative_values"
      rejectsNeg.coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .option("quoteAll", "true")
        .csv(rejectsCsvOut)
      println(s"Rejects written to: $rejectsCsvOut")

      currentDF = cleanedNeg
    } else {
      println("No numeric columns found — skipping no_negative_values check.")
    }

    // Step 6: Final cleaned dataset metrics
    val finalCount = currentDF.count()

    println("\n=== FINAL CLEANED DATASET METRICS ===")
    println(f"Initial total rows: $totalRows%,d")
    println(f"Final kept rows:    $finalCount%,d")
    println(f"Total removed:      ${totalRows - finalCount}%,d")

    // Step 7: Write final cleaned Parquet
    currentDF
      .coalesce(1)
      .write
      .mode("overwrite")
      .option("compression", "snappy")
      .parquet(SilverTripsOut)

    println(s"✅ Final cleaned data written to: $SilverTripsOut")

    // Rename part file
    val outDir = new File(SilverTripsOut)
    val partFile = outDir.listFiles().find(_.getName.startsWith("part-")).get
    val renamed = new File(outDir, "clean_trips_2025_01.parquet")
    Files.move(partFile.toPath, renamed.toPath, StandardCopyOption.REPLACE_EXISTING)
    println(s"✅ Renamed Parquet file to: ${renamed.getAbsolutePath}")

    // Step 8: Write 500K sample CSV snapshot
    val sampleCsvOut = s"$SilverRoot/clean_sample_500k"
    println("\n=== WRITING 500K ROW SAMPLE CSV ===")
    val sampleCount = Math.min(500000, finalCount.toInt)
    currentDF.orderBy(rand())
      .limit(sampleCount)
      .coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .option("quoteAll", "true")
      .csv(sampleCsvOut)
    println(s"✅ 500K-row sample CSV written to: $sampleCsvOut")

    // Step 9: Show preview
    println("\n=== SAMPLE CLEAN ROWS ===")
    currentDF.select(
      col("tpep_pickup_datetime"),
      col("tpep_dropoff_datetime"),
      col("trip_duration_min"),
      col("passenger_count"),
      col("total_amount"),
      col("trip_distance"),
      col("payment_type")
    ).show(10, truncate = false)

    spark.stop()
  }
}
