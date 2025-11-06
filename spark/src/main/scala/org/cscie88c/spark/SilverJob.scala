package org.cscie88c.spark

import org.apache.spark.sql.{SparkSession, DataFrame, Column}
import org.apache.spark.sql.functions._


object SilverJob {

  val TripsParquetPath  = "/opt/spark-data/yellow_tripdata_2025-01.parquet"
  val TaxiZonesCsvPath  = "/opt/spark-data/taxi_zone_lookup.csv"

  val SilverRoot        = "/opt/spark-data/silver"
  val SilverTripsOut    = s"$SilverRoot/trips_conformed"
  val dqThreshold       = 0.20   // 20% reject threshold per DQ rule



  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("SilverTripCleaning-MultiRule-SingleParquet")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._
    val outputFiles:Boolean = false
    cleanData(TripsParquetPath, TaxiZonesCsvPath, outputFiles)(spark)
    spark.stop()

  }
  private def applyBasicDQRules(df: DataFrame, outputExtraFiles: Boolean): DataFrame = {
    var currentDF = df
    currentDF = applyDQRule(currentDF, "Non-null", currentDF.columns.map(c => col(c).isNotNull).reduce(_ && _), outputExtraFiles)
    currentDF = applyDQRule(currentDF, "passenger_count_valid", col("passenger_count").between(1, 8), outputExtraFiles)
    currentDF = applyDQRule(currentDF, "total_amount_nonnegative", col("total_amount") >= 0, outputExtraFiles)
    currentDF = applyDQRule(currentDF, "chronological_order_valid", col("tpep_dropoff_datetime") >= col("tpep_pickup_datetime"), outputExtraFiles)
    currentDF = applyDQRule(currentDF, "duration_reasonable", col("trip_duration_min").between(1.0, 180.0), outputExtraFiles)
    currentDF = applyDQRule(currentDF, "distance_reasonable", col("trip_distance").between(0.1, 100.0), outputExtraFiles)
    currentDF = applyDQRule(currentDF, "payment_type_valid", col("payment_type").between(1, 6), outputExtraFiles)
    currentDF = applyDQRule(currentDF, "ratecode_valid", col("RatecodeID").between(1, 6), outputExtraFiles)
    currentDF
  }
  def cleanData(tripsParquetPath: String, taxiZonesCsvPath: String,  outputExtraFiles: Boolean)(spark: SparkSession) : Unit = {
    // ============================================================
    // Step 1: Load input
    // ============================================================
    val tripsDF = TripData.loadParquetData(tripsParquetPath)(spark)
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

    val currentDF = withDerived
    val totalRows = currentDF.count()

    // ============================================================
    // Step 3: Data Quality Pipeline
    // ============================================================
    println("\n=== STARTING DQ CLEANING PIPELINE ===")
    val withBasicDQ = applyBasicDQRules(currentDF)

    // ============================================================
    // Step 4: Remove negative numeric values
    // ============================================================
    println("\n=== APPLYING RULE: no_negative_values ===")
    val numericCols = withBasicDQ.schema.fields.filter(f =>
      f.dataType.simpleString.matches("int|double|float|long|decimal.*")
    ).map(_.name)
    var finalCleanCount = withBasicDQ.count()

    if (numericCols.nonEmpty) {
      val negativeCondition = numericCols.map(c => col(c) >= 0).reduce(_ && _)
      val numericallyCleanDF = applyDQRule(withBasicDQ, "no_negative_values", negativeCondition, outputExtraFiles)
      finalCleanCount = numericallyCleanDF.count()
    } else {
      println("No numeric columns found — skipping no_negative_values check.")
    }


    println("\n=== FINAL CLEANED DATASET METRICS ===")
   println(f"Initial rows: $totalRows%,d | Final kept: $finalCleanCount%,d | Removed: ${totalRows - finalCleanCount}%,d")

    // ============================================================
    // Step 5: Write 10K-row sample after cleaning
    // ============================================================
    println("\n=== STEP 7: WRITING 10K SAMPLE AFTER CLEANING ===")
    if (outputExtraFiles) {
      val sampleCsvOutClean = s"$SilverRoot/clean_sample_10k"
      val sampleCountClean = Math.min(10000, finalCleanCount.toInt)
      Utilities.csvOutput(currentDF, sampleCsvOutClean, sampleCountClean, rand())
       println(f"✅ 10K-row clean sample CSV written to: $sampleCsvOutClean ($sampleCountClean%,d rows)")
    }




    // ============================================================
    // Step 6: Clean Taxi Zone Lookup
    // ============================================================
     println("\n=== STARTING ZONE LOOKUP CLEANING ===")
    val rawZonesDF = TaxiZones.zonesFromFile(taxiZonesCsvPath)(spark)

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

    if(outputExtraFiles){
      val cleanZonesOut = s"$SilverRoot/taxi_zone_lookup_clean"

      Utilities.csvOutput(cleanedZonesDF, cleanZonesOut, keptZones.toInt, col("LocationID"))
      println(s"✅ Cleaned Taxi Zones written to: $cleanZonesOut")
    }


    // ============================================================
    // Step 7: Conformance & Joins (Standardize to EST + Weekly Bucketing)
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
    // Step 8: Remove Null Boroughs
    // ============================================================
    println("\n=== APPLYING POST JOIN DQ RULE: drop_invalid_zone_ids ===")
    //pickup borough must be non null and not an empty string
    val condition: Column = not(col("pickup_borough").isNull || trim(col("pickup_borough")) === "")
    val finalConformedDF = applyDQRule(joinedTripsDF, "Invalid Zones", condition ,outputExtraFiles)



      // ============================================================
      // Step 9: Write conformed Silver Parquet
      // ============================================================
      Utilities.parquetOutput(finalConformedDF, SilverTripsOut)
      println(s"✅ Conformed Silver data written to: $SilverTripsOut")



    if (outputExtraFiles) {
      // ============================================================
      // Step 10: Write 10K-row sample after conformance
      // ============================================================

      println("\n=== STEP 10: WRITING 10K SAMPLE AFTER CONFORMANCE ===")
      val sampleCsvOutConf = s"$SilverRoot/conformed_sample_10k"
      val sampleCountConf = Math.min(10000, joinedTripsDF.count().toInt)

      Utilities.csvOutput(joinedTripsDF, sampleCsvOutConf, sampleCountConf, rand())
       println(f"✅ 10K-row conformed sample CSV written to: $sampleCsvOutConf ($sampleCountConf%,d rows)")
    }
    // ============================================================
    // Step 11: Preview
    // ============================================================

      println("\n=== SAMPLE CONFORMED ROWS PREVIEW ===")

      joinedTripsDF
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


  }

  def applyDQRule(df: DataFrame, ruleName: String, condition: org.apache.spark.sql.Column, outputFiles: Boolean): DataFrame = {
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
      throw new RuntimeException(f"❌ FAIL-FAST: Rule [$ruleName] reject rate ${rejectRate * 100}%.2f%% exceeds ${dqThreshold * 100}%.1f%% threshold.")

    if (outputFiles) {
      val rejectsCsvOut = s"$SilverRoot/rejects_${ruleName}"
      csvOutput(rejects,rejectsCsvOut,rejectedCount.toInt,col("tpep_pickup_datetime_est"))
      println(s"Rejects written to: $rejectsCsvOut")
    }



    cleaned
  }



}
