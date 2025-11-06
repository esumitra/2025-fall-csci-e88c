package org.cscie88c.spark

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import java.io.File
import java.nio.file.{Files, Paths, StandardCopyOption}

object SilverJob {

  val TripsParquetPath  = "/opt/spark-data/yellow_tripdata_2025-01.parquet"
  val TaxiZonesCsvPath  = "/opt/spark-data/taxi_zone_lookup.csv"

  val SilverRoot        = "/opt/spark-data/silver"
  val SilverTripsOut    = s"$SilverRoot/trips_conformed"
  val dqThreshold       = 0.20   // 20% reject threshold

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("SilverTripCleaning-MultiRule-SingleParquet")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    // ============================================================
    // Step 1: Load input
    // ============================================================
    val tripsDF = spark.read.parquet(TripsParquetPath)
    println(s"=== DEBUG: Raw input count === ${tripsDF.count()}")

    // ============================================================
    // Step 2: Derived features
    // ============================================================
    val withDerived = tripsDF
      .withColumn(
        "trip_duration_min",
        (col("tpep_dropoff_datetime").cast("long") - col("tpep_pickup_datetime").cast("long")) / 60.0
      )
      .withColumn("pickup_date", to_date(col("tpep_pickup_datetime")))
      .withColumn("pickup_hour", hour(col("tpep_pickup_datetime")))

    println("=== DEBUG: Sample trip_duration_min values ===")
    withDerived
      .select("tpep_pickup_datetime", "tpep_dropoff_datetime", "trip_duration_min")
      .show(10, truncate = false)

    var currentDF = withDerived
    val totalRows = currentDF.count()

    // ============================================================
    // Step 3: Remove nulls upfront
    // ============================================================
    println("\n=== APPLYING INITIAL RULE: no_null_values ===")
    val rejectsNulls = currentDF.filter(currentDF.columns.map(c => col(c).isNull).reduce(_ || _))
    val cleanedNoNulls = currentDF.filter(currentDF.columns.map(c => col(c).isNotNull).reduce(_ && _))

    val totalBeforeNulls = currentDF.count()
    val rejectedNulls = rejectsNulls.count()
    val keptNoNulls = cleanedNoNulls.count()
    val nullRejectRate = rejectedNulls.toDouble / totalBeforeNulls

    println(f"=== METRICS: no_null_values ===")
    println(f"Total before: $totalBeforeNulls%,d | Kept: $keptNoNulls%,d | Rejected: $rejectedNulls%,d (${nullRejectRate * 100}%.2f%%)")

    if (nullRejectRate > dqThreshold)
      throw new RuntimeException(f"❌ FAIL-FAST: Nulls exceed ${dqThreshold * 100}%.1f%%")

    val rejectsCsvOutInit = s"$SilverRoot/rejects_no_null_values"
    rejectsNulls.coalesce(1).write.mode("overwrite").option("header", "true").option("quoteAll", "true").csv(rejectsCsvOutInit)
    println(s"Rejects written to: $rejectsCsvOutInit")

    currentDF = cleanedNoNulls

    // ============================================================
    // Step 4: Helper to apply DQ rules
    // ============================================================
    def applyDQRule(df: DataFrame, ruleName: String, condition: org.apache.spark.sql.Column): DataFrame = {
      println(s"\n=== APPLYING RULE: $ruleName ===")

      val rejects = df.filter(!condition)
      val cleaned = df.filter(condition)

      val totalBefore = df.count()
      val rejectedCount = rejects.count()
      val keptCount = cleaned.count()
      val rejectRate = rejectedCount.toDouble / totalBefore

      println(f"=== METRICS: $ruleName ===")
      println(f"Total before: $totalBefore%,d | Kept: $keptCount%,d | Rejected: $rejectedCount%,d (${rejectRate * 100}%.2f%%)")

      if (rejectRate > dqThreshold)
        throw new RuntimeException(f"❌ FAIL-FAST: Rule [$ruleName] reject rate ${rejectRate * 100}%.2f%% exceeds limit.")

      val rejectsCsvOut = s"$SilverRoot/rejects_${ruleName}"
      rejects.coalesce(1).write.mode("overwrite").option("header", "true").option("quoteAll", "true").csv(rejectsCsvOut)
      println(s"Rejects written to: $rejectsCsvOut")

      cleaned
    }

    // ============================================================
    // Step 5: DQ Pipeline
    // ============================================================
    println("\n=== STARTING DQ CLEANING PIPELINE ===")

    currentDF = applyDQRule(currentDF, "passenger_count_valid", col("passenger_count").between(1, 8))
    currentDF = applyDQRule(currentDF, "total_amount_nonnegative", col("total_amount") >= 0)
    currentDF = applyDQRule(currentDF, "chronological_order_valid", col("tpep_dropoff_datetime") >= col("tpep_pickup_datetime"))
    currentDF = applyDQRule(currentDF, "duration_reasonable", col("trip_duration_min").between(1.0, 180.0))
    currentDF = applyDQRule(currentDF, "distance_reasonable", col("trip_distance").between(0.1, 100.0))
    currentDF = applyDQRule(currentDF, "payment_type_valid", col("payment_type").between(1, 6))
    currentDF = applyDQRule(currentDF, "ratecode_valid", col("RatecodeID").between(1, 6))

    // ============================================================
    // Step 6: No negative numeric values
    // ============================================================
    println("\n=== APPLYING RULE: no_negative_values ===")
    val numericCols = currentDF.schema.fields.filter(f =>
      f.dataType.simpleString.matches("int|double|float|long|decimal.*")
    ).map(_.name)

    if (numericCols.nonEmpty) {
      val negativeCondition = numericCols.map(c => col(c) >= 0).reduce(_ && _)
      currentDF = applyDQRule(currentDF, "no_negative_values", negativeCondition)
    } else {
      println("No numeric columns found — skipping no_negative_values")
    }

    val finalCleanCount = currentDF.count()
    println("\n=== FINAL CLEANED DATASET METRICS ===")
    println(f"Initial rows: $totalRows%,d | Final kept: $finalCleanCount%,d | Removed: ${totalRows - finalCleanCount}%,d")

    // ============================================================
    // Step 7: 10K Clean Sample
    // ============================================================
    println("\n=== STEP 7: WRITING 10K SAMPLE AFTER CLEANING ===")
    val sampleCsvOutClean = s"$SilverRoot/clean_sample_10k"
    val sampleCountClean = Math.min(10000, finalCleanCount.toInt)

    currentDF.orderBy(rand()).limit(sampleCountClean)
      .coalesce(1).write.mode("overwrite").option("header", "true").option("quoteAll", "true").csv(sampleCsvOutClean)

    println(f"✅ Clean sample written to: $sampleCsvOutClean")

    // ============================================================
    // Step 8: Clean Taxi Zones
    // ============================================================
    println("\n=== STARTING ZONE LOOKUP CLEANING ===")

    val rawZonesDF = TaxiZones.zonesFromFile(TaxiZonesCsvPath)(spark)

    val cleanedZonesDF = rawZonesDF.filter(
      rawZonesDF.columns.map { c =>
        lower(trim(col(c))) =!= "n/a" &&
        lower(trim(col(c))) =!= "unknown" &&
        lower(trim(col(c))) =!= "outside of nyc" &&
        col(c).isNotNull &&
        trim(col(c)) =!= ""
      }.reduce(_ && _)
    )

    val totalZones = rawZonesDF.count()
    val keptZones = cleanedZonesDF.count()
    println(f"✅ Cleaned zones: $keptZones%,d / $totalZones%,d")

    val cleanZonesOut = s"$SilverRoot/taxi_zone_lookup_clean"
    cleanedZonesDF.coalesce(1).write.mode("overwrite").option("header", "true").option("quoteAll", "true").csv(cleanZonesOut)
    println(s"✅ Cleaned Taxi Zones written to: $cleanZonesOut")

    // ============================================================
    // Step 9: Conformance + EST + Week Buckets
    // ============================================================
    println("\n=== STEP 9: CONFORMANCE & JOINS (EST + WEEK BUCKETING) ===")

    val conformedTripsDF = currentDF
      .withColumn("tpep_pickup_datetime_est",
        from_utc_timestamp(to_utc_timestamp(col("tpep_pickup_datetime"), "UTC"), "America/New_York"))
      .withColumn("tpep_dropoff_datetime_est",
        from_utc_timestamp(to_utc_timestamp(col("tpep_dropoff_datetime"), "UTC"), "America/New_York"))
      .withColumn("pickup_week", weekofyear(col("pickup_date")))
      .withColumn("pickup_week_start", date_sub(next_day(col("pickup_date"), "Mon"), 7))
      .withColumn("pickup_day", date_format(col("pickup_date"), "yyyy-MM-dd"))
      .withColumn("pickup_hour_str", format_string("%02d:00", col("pickup_hour")))
      .withColumn("day_period",
        when(col("pickup_hour").between(5, 11), "Morning")
          .when(col("pickup_hour").between(12, 17), "Afternoon")
          .when(col("pickup_hour").between(18, 22), "Evening")
          .otherwise("Night")
      )

    val joinedTripsDF = conformedTripsDF.join(cleanedZonesDF,
      conformedTripsDF("PULocationID") === cleanedZonesDF("LocationID"), "left")
      .withColumnRenamed("Borough", "pickup_borough")
      .withColumnRenamed("Zone", "pickup_zone")
      .withColumnRenamed("service_zone", "pickup_service_zone")
      .drop("LocationID")

    println(f"✅ Joined trips + zones: ${joinedTripsDF.count()}%,d rows")

    // ============================================================
    // Step 9B: Remove Null Boroughs
    // ============================================================
    println("\n=== APPLYING FINAL DQ RULE: drop_invalid_zone_ids ===")

    val invalidZoneTrips = joinedTripsDF.filter(col("pickup_borough").isNull || trim(col("pickup_borough")) === "")
    val validZoneTrips   = joinedTripsDF.filter(col("pickup_borough").isNotNull && trim(col("pickup_borough")) =!= "")

    val totalZoneBefore = joinedTripsDF.count()
    val removedZone = invalidZoneTrips.count()
    val keptZone = validZoneTrips.count()

    println(f"=== METRICS: drop_invalid_zone_ids ===")
    println(f"Total before: $totalZoneBefore%,d | Kept: $keptZone%,d | Removed: $removedZone%,d (${removedZone.toDouble/totalZoneBefore*100}%.2f%%)")

    val rejectsZoneOut = s"$SilverRoot/rejects_invalid_zone_ids"
    invalidZoneTrips.coalesce(1).write.mode("overwrite").option("header", "true").option("quoteAll", "true").csv(rejectsZoneOut)
    println(s"Rejects written to: $rejectsZoneOut")

    val finalConformedDF = validZoneTrips

    // ============================================================
    // Step 10: Write Conformed Parquet
    // ============================================================
    finalConformedDF.coalesce(1)
      .write.mode("overwrite")
      .option("compression", "snappy")
      .parquet(SilverTripsOut)

    println(s"✅ Conformed Silver data written to: $SilverTripsOut")

    // ============================================================
    // Step 11: 10K Conformed Sample
    // ============================================================
    println("\n=== STEP 11: WRITING 10K SAMPLE AFTER CONFORMANCE ===")
    val sampleCsvOutConf = s"$SilverRoot/conformed_sample_10k"
    val totalConformed = finalConformedDF.count()
    val sampleCountConf = Math.min(10000, totalConformed.toInt)

    finalConformedDF.orderBy(rand()).limit(sampleCountConf)
      .coalesce(1).write.mode("overwrite").option("header", "true").option("quoteAll", "true").csv(sampleCsvOutConf)

    println(f"✅ 10K-row conformed sample CSV written to: $sampleCsvOutConf ($sampleCountConf%,d rows)")

    // ============================================================
    // Step 12: Preview
    // ============================================================
    println("\n=== SAMPLE CONFORMED ROWS PREVIEW ===")
    finalConformedDF
      .select(
        col("tpep_pickup_datetime_est"),
        col("tpep_dropoff_datetime_est"),
        col("pickup_week"),
        col("pickup_week_start"),
        col("pickup_zone"),
        col("passenger_count"),
        col("total_amount")
      )
      .show(10, truncate = false)

    spark.stop()
  }
}
