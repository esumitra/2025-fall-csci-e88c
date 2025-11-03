package org.cscie88c.spark

import java.sql.Timestamp
import org.apache.spark.sql.{SparkSession, Dataset, functions => F}

case class TaxiTrip(
  VendorID: Option[Long],
  tpep_pickup_datetime: Option[Timestamp],
  tpep_dropoff_datetime: Option[Timestamp],
  passenger_count: Option[Long],
  trip_distance: Option[Double],
  RatecodeID: Option[Long],
  store_and_fwd_flag: Option[String],
  PULocationID: Option[Long],
  DOLocationID: Option[Long],
  payment_type: Option[Long],
  fare_amount: Option[Double],
  extra: Option[Double],
  mta_tax: Option[Double],
  tip_amount: Option[Double],
  tolls_amount: Option[Double],
  improvement_surcharge: Option[Double],
  total_amount: Option[Double],
  congestion_surcharge: Option[Double],
  airport_fee: Option[Double]
)

case class ProjectKPIs(
  week_start: String,
  borough: String,
  trip_volume: Long,
  total_trips: Long,
  total_revenue: Double,
  per_hour_trip_percentage: Double,
  avg_trip_time_vs_distance: Long,
  avg_revenue_per_mile: Double,
  night_trip_percentage: Long
)

/**
●	Reproducible environment: sbt project, README with make/run commands. Private fork the student repository with access granted to team members and staff.

●	Documentation: KPI definitions (exact formulas)

  */

object SparkJob {
  def main(args: Array[String]): Unit = {
    val Array(infile, outpath) = args
    implicit val spark = SparkSession.builder()
      .appName("Week8GroupProject")
      // do not hardcode master here so spark-submit's --master is respected
      .getOrCreate()

    // Basic run logging to help debugging when running inside Docker
    println(s"[SparkJob] args=${args.mkString(" ")}")
    spark.sparkContext.setLogLevel("WARN")

    try {
      // 1. Load input file (bronze layer)
      println(s"[SparkJob] Loading input: $infile")
      val inputMovieData: Dataset[TaxiTrip] = loadInputFile(infile)
      println(s"[SparkJob] Loaded input rows=${inputMovieData.count}")

      // 2. Cleanup data (silver layer)
      println("[SparkJob] Cleaning data")
      val cleanMovieData: Dataset[TaxiTrip] = cleanData(inputMovieData)
      println(s"[SparkJob] Cleaned rows=${cleanMovieData.count}")

        // 3. Calculate aggregate KPIs (gold layer)
        println("[SparkJob] Calculating KPIs")
        // optional 3rd arg: number of weeks to include (default 4)
        val weeksToInclude: Int = if (args.length >= 3) try { args(2).toInt } catch { case _: Throwable => 4 } else 4
        val projectKPIs: Dataset[ProjectKPIs] = calculateKPIs(cleanMovieData, weeksToInclude, outpath)
      println(s"[SparkJob] KPIs rows=${projectKPIs.count}")
      projectKPIs.show(false)

      // 4. Save output
  val kpisOut = if (outpath.endsWith("/")) outpath + "kpis" else outpath + "/kpis"
  println(s"[SparkJob] Saving KPIs to: $kpisOut")
  saveOutput(projectKPIs, kpisOut)
  println("[SparkJob] Save complete")

      spark.stop()
    } catch {
      case t: Throwable =>
        println(s"[SparkJob][ERROR] Job failed: ${t.getMessage}")
        t.printStackTrace()
        try { spark.stop() } catch { case _: Throwable => }
        // rethrow to ensure spark-submit sees a non-zero exit
        System.exit(1)
    }
  }

  def loadInputFile(filePath: String)(implicit spark: SparkSession): Dataset[TaxiTrip] = {
    import spark.implicits._
    
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .parquet(filePath)
      .as[TaxiTrip]
  }

  def cleanData(inputData: org.apache.spark.sql.Dataset[TaxiTrip]): org.apache.spark.sql.Dataset[TaxiTrip] = {
    // Use DataFrame APIs for clearer edge-case handling and better performance.
    // This function now performs several data-quality (DQ) checks and will fail-fast on critical issues.
    import inputData.sparkSession.implicits._
    val df = inputData.toDF()

    // --- Basic checks (fail-fast on critical issues) ---
    val totalRows = df.count()

    // Required columns for downstream processing
    val requiredCols = Seq(
      "tpep_pickup_datetime",
      "tpep_dropoff_datetime",
      "passenger_count",
      "trip_distance",
      "fare_amount",
      "total_amount"
    )
    val missingCols = requiredCols.filterNot(df.columns.contains)
    if (missingCols.nonEmpty) {
      throw new IllegalArgumentException(s"Missing required columns: ${missingCols.mkString(", ")}")
    }

    // Null-rate checks: fail if any critical column has null rate > threshold
    val nullRateThreshold = 0.20 // 20%
    val nullRates = requiredCols.map { c =>
      val nulls = df.filter(F.col(c).isNull).count()
      val rate = if (totalRows == 0) 1.0 else nulls.toDouble / totalRows
      (c, rate)
    }
    val highNulls = nullRates.filter { case (_, rate) => rate > nullRateThreshold }
    if (highNulls.nonEmpty) {
      val msg = highNulls.map { case (c, r) => s"$c:${(r * 100)}%" }.mkString(", ")
      throw new RuntimeException(s"High null rates detected for critical columns: $msg")
    }

    // Unique ID check if an ID column exists (fail-fast if duplicates)
    if (df.columns.contains("trip_id")) {
      val dupCount = df.groupBy("trip_id").count().filter(F.col("count") > 1).limit(1).count()
      if (dupCount > 0) {
        throw new RuntimeException("Duplicate trip_id values found (unique ID constraint violated)")
      }
    }

    // Completeness by week: ensure there are no missing weeks between min and max pickup date
    val dfWithTs = df.withColumn("pickup_ts", F.col("tpep_pickup_datetime").cast("timestamp"))
    val minPickupOpt = dfWithTs.agg(F.min("pickup_ts").as("min_ts")).select("min_ts").as[java.sql.Timestamp].collect().headOption
    val maxPickupOpt = dfWithTs.agg(F.max("pickup_ts").as("max_ts")).select("max_ts").as[java.sql.Timestamp].collect().headOption
    if (minPickupOpt.isEmpty || maxPickupOpt.isEmpty) {
      throw new RuntimeException("No valid pickup timestamps found for completeness check")
    }
    val minDate = minPickupOpt.get.toInstant.atZone(java.time.ZoneId.systemDefault()).toLocalDate
    val maxDate = maxPickupOpt.get.toInstant.atZone(java.time.ZoneId.systemDefault()).toLocalDate
    val weeksBetween = java.time.temporal.ChronoUnit.WEEKS.between(minDate, maxDate).toInt + 1

    // Spark 3.x removed support for week-based patterns in DateTimeFormatter (e.g. 'w' or 'ww').
    // Build a stable week identifier using year() and weekofyear() instead.
    val weeksPresent = dfWithTs
      .withColumn("week", F.concat_ws("-",
        F.year(F.col("pickup_ts")).cast("string"),
        F.lpad(F.weekofyear(F.col("pickup_ts")).cast("string"), 2, "0")
      ))
      .select("week").distinct().as[String].collect().toSet

    if (weeksPresent.size < weeksBetween) {
      throw new RuntimeException(s"Data is not complete by week: present weeks=${weeksPresent.size}, expected weeks=$weeksBetween")
    }

    // --- Row-level filters (cleaning) ---
    // Filters applied:
    // - fare_amount, total_amount non-null and >= 0
    // - trip_distance non-null and > 0 (drop zero-distance trips)
    // - passenger_count between 0 and 8 (reasonable taxi capacity)
    // - pickup and dropoff timestamps present and pickup <= dropoff
    // - tip_amount non-negative when present
    // - payment_type is either null or within an expected range (1-6)
    val cleaned = df
      .filter(F.col("fare_amount").isNotNull && F.col("fare_amount") >= 0)
      .filter(F.col("total_amount").isNotNull && F.col("total_amount") >= 0)
      .filter(F.col("trip_distance").isNotNull && F.col("trip_distance") > 0)
      .filter(F.col("passenger_count").isNotNull && F.col("passenger_count").between(0, 8))
      .filter(F.col("tpep_pickup_datetime").isNotNull && F.col("tpep_dropoff_datetime").isNotNull)
      // create timestamp cols to compare pickup/dropoff
      .withColumn("pickup_ts", F.col("tpep_pickup_datetime").cast("timestamp"))
      .withColumn("dropoff_ts", F.col("tpep_dropoff_datetime").cast("timestamp"))
      .filter(F.col("pickup_ts").isNotNull && F.col("dropoff_ts").isNotNull && F.col("dropoff_ts") >= F.col("pickup_ts"))
      .filter(F.col("tip_amount").isNull || F.col("tip_amount") >= 0)
      .filter(F.col("payment_type").isNull || F.col("payment_type").between(1, 6))
      .drop("pickup_ts", "dropoff_ts")

    // Convert back to the typed Dataset
    cleaned.as[TaxiTrip]
  }

  def calculateKPIs(inputData: org.apache.spark.sql.Dataset[TaxiTrip],
                    weeks: Int = 4,
                    outputPath: String)(implicit spark: SparkSession): org.apache.spark.sql.Dataset[ProjectKPIs] = {
    import spark.implicits._
  // Compute total trips (used to convert counts to percentages)

    // Safely extract pickup hour (attempt to cast to Spark timestamp)
    // and filter to the requested recent weeks range (based on the dataset max pickup timestamp)
    val withTs = inputData
      .withColumn("pickup_ts", F.col("tpep_pickup_datetime").cast("timestamp"))
      .withColumn("hour", F.hour(F.col("pickup_ts")))

    // Determine cutoff (weeks) relative to max pickup timestamp
    val maxPickupOpt = withTs.agg(F.max("pickup_ts").as("max_ts")).select("max_ts").as[java.sql.Timestamp].collect().headOption
    val cutoffTsLiteral = maxPickupOpt.map { ts =>
      val cutoffMillis = ts.getTime - (weeks.toLong * 7L * 24L * 60L * 60L * 1000L)
      new java.sql.Timestamp(Math.max(0L, cutoffMillis))
    }

    val filtered = cutoffTsLiteral match {
      case Some(cutoffTs) =>
        withTs.filter(F.col("pickup_ts").isNotNull && F.col("pickup_ts") >= F.lit(cutoffTs))
      case None => withTs
    }

    val withHour = filtered

  // Trips per hour and percentage of total trips per hour (use filtered total)
  val totalTrips: Long = withHour.count()
  val tripsByHour = withHour.groupBy("hour").count()
  val tripsByHourPct = if (totalTrips > 0) tripsByHour.withColumn("percentage", F.col("count") / F.lit(totalTrips) * 100) else tripsByHour.withColumn("percentage", F.lit(0))

    // Peak-hour percentage (percentage of trips that occurred in the busiest hour)
    val peakPercentage: Double = tripsByHourPct.agg(F.max("percentage").as("max_pct"))
      .select("max_pct").as[Double].collect().headOption.getOrElse(0.0)

    // Average revenue per mile (guard against zero or null distances) over filtered range
    val avgRevenuePerMile: Double = withHour.filter(F.col("trip_distance") > 0)
      .agg(F.avg(F.col("total_amount") / F.col("trip_distance")).as("avg_rev_per_mile"))
      .select("avg_rev_per_mile").as[Double].collect().headOption.getOrElse(0.0)

  // Night trip count (define night as hours 0-5 inclusive)
  val nightTripCount: Long = withHour.filter(F.col("hour") >= 0 && F.col("hour") < 6).count()

    // Calculate average trip time (in minutes) per mile
    val withTripTime = withHour
      .withColumn("pickup_ts", F.col("tpep_pickup_datetime").cast("timestamp"))
      .withColumn("dropoff_ts", F.col("tpep_dropoff_datetime").cast("timestamp"))
      .withColumn("trip_minutes", 
        (F.unix_timestamp(F.col("dropoff_ts")) - F.unix_timestamp(F.col("pickup_ts"))) / 60.0)
      .filter(F.col("trip_distance") > 0) // avoid divide by zero
      .withColumn("minutes_per_mile", F.col("trip_minutes") / F.col("trip_distance"))

    val avgTripTimeVsDistance: Long = withTripTime
      .agg(F.avg("minutes_per_mile").cast("long").as("avg_time_per_mile"))
      .select("avg_time_per_mile")
      .as[Long]
      .collect()
      .headOption
      .getOrElse(0L)

    // --- Weekly metrics by borough ---
    // No external borough lookup supplied in the repository; fall back to using PULocationID as a borough placeholder.
    val dfWithBorough = withHour.withColumn("borough",
      F.when(F.col("PULocationID").isNotNull, F.concat(F.lit("PULocation_"), F.col("PULocationID").cast("string"))).otherwise(F.lit("UNKNOWN")))

    // compute a week_start (date string) using date_trunc to get the week's starting date
    val dfWithWeek = dfWithBorough.withColumn("week_start", F.date_format(F.date_trunc("week", F.col("pickup_ts")), "yyyy-MM-dd"))

    // Calculate weekly totals by borough
    val weeklyByBorough = dfWithWeek
      .groupBy("week_start", "borough")
      .agg(
        F.count("*").as("trip_volume"),
        F.sum("total_amount").as("borough_revenue")
      )

    // Calculate weekly totals across all boroughs
    val weeklyTotals = dfWithWeek
      .groupBy("week_start")
      .agg(
        F.count("*").as("total_trips"),
        F.sum("total_amount").as("total_revenue")
      )

    // Join the borough-level and total metrics
    val weeklyMetrics = weeklyByBorough
      .join(weeklyTotals, Seq("week_start"))
      .select(
        F.col("week_start"),
        F.col("borough"),
        F.col("trip_volume"),
        F.col("total_trips"),
        F.col("total_revenue"),
        F.lit(peakPercentage).as("per_hour_trip_percentage"),
        F.lit(avgTripTimeVsDistance).as("avg_trip_time_vs_distance"),
        F.lit(avgRevenuePerMile).as("avg_revenue_per_mile"),
        F.lit(nightTripCount).as("night_trip_percentage")
      )
      .withColumn("per_hour_trip_percentage", F.lit(peakPercentage))
      .withColumn("avg_trip_time_vs_distance", F.lit(avgTripTimeVsDistance))
      .withColumn("avg_revenue_per_mile", F.lit(avgRevenuePerMile))
      .withColumn("night_trip_percentage", F.lit(nightTripCount))
      .as[ProjectKPIs]

    // Write out detailed weekly metrics
    weeklyMetrics.write.mode("overwrite").parquet(outputPath + "/weekly_metrics")

    // Return the full dataset of KPIs
    weeklyMetrics
  }

  def saveOutput(kpis: org.apache.spark.sql.Dataset[ProjectKPIs], outputPath: String): Unit = {
    kpis.write
      .mode("overwrite")
      .parquet(outputPath)
  }
}
