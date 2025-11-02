package org.cscie88c.spark

import com.google.protobuf.Timestamp
import org.apache.spark.sql.{SparkSession, Dataset, functions => F}

case class TaxiTrip(
  VendorID: Option[Int],
  tpep_pickup_datetime: Option[Timestamp],
  tpep_dropoff_datetime: Option[Timestamp],
  passenger_count: Option[Int],
  trip_distance: Option[Double],
  RatecodeID: Option[Int],
  store_and_fwd_flag: Option[String],
  PULocationID: Option[Int],
  DOLocationID: Option[Int],
  payment_type: Option[Int],
  fare_amount: Option[Double],
  extra: Option[Double],
  mta_tax: Option[Double],
  tip_amount: Option[Double],
  tolls_amount: Option[Double],
  improvement_surcharge: Option[Double],
  total_amount: Option[Double],
  congestion_surcharge: Option[Double],
  airport_fee: Option[Double],
  trip_type: Option[Int]
)

case class ProjectKPIs(
  per_hour_trip_percentage: Double,
  weekly_trip_volume_by_borough: Long,
  avg_trip_time_vs_distance: Long,
  weekly_total_trips_and_total_revenue: Double,
  avg_revenue_per_mile: Double,
  night_trip_percentage: Long
)

/**
  * Functional requirements:
    ●	Batch only (no streaming). Pipeline runs for a defined date range (i.e., last 4 weeks).

●	Reproducible environment: sbt project, README with make/run commands. Private fork the student repository with access granted to team members and staff.

●	Data quality checks (DQ): schema, null rates, unique IDs where applicable, valid ranges (e.g., fare ≥ 0), and completeness by week. Fail fast on critical errors.

●	Transformations: time bucketing to week, dimensional join(s) (e.g., taxi zone lookup)

●	Outputs:

○	Bronze: raw (optionally corrected)

○	Silver: cleaned & conformed

○	Gold: aggregate KPIs


●	Tests: unit tests for transforms; dataset-level checks for KPIs. Unit tests should not run spark.

●	Documentation: KPI definitions (exact formulas)

  */

object SparkJob {
  def main(args: Array[String]): Unit = {
    val Array(infile, outpath) = args
    implicit val spark = SparkSession.builder()
      .appName("Week8GroupProject")
      .master("local[*]")
      .getOrCreate()

    // 1. Load input file (bronze layer)
    val inputMovieData: Dataset[TaxiTrip] = loadInputFile(infile)

    // 2. Cleanup data (silver layer)
    val cleanMovieData: Dataset[TaxiTrip] = cleanData(inputMovieData)

    // 3. Calculate aggregate KPIs (gold layer)
    val projectKPIs: Dataset[ProjectKPIs] = calculateKPIs(cleanMovieData)

    // 4. Save output
    saveOutput(projectKPIs, outpath)

    spark.stop()
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

    val weeksPresent = dfWithTs
      .withColumn("week", F.date_format(F.col("pickup_ts"), "yyyy-ww"))
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

  def calculateKPIs(inputData: org.apache.spark.sql.Dataset[TaxiTrip])(implicit spark: SparkSession): org.apache.spark.sql.Dataset[ProjectKPIs] = {
    import spark.implicits._
    // Compute total trips (used to convert counts to percentages)
    val totalTrips: Long = inputData.count()

    // Safely extract pickup hour (attempt to cast to Spark timestamp)
    val withHour = inputData
      .withColumn("pickup_ts", F.col("tpep_pickup_datetime").cast("timestamp"))
      .withColumn("hour", F.hour(F.col("pickup_ts")))

    // Trips per hour and percentage of total trips per hour
    val tripsByHour = withHour.groupBy("hour").count()
    val tripsByHourPct = tripsByHour.withColumn("percentage", F.col("count") / F.lit(totalTrips) * 100)

    // Peak-hour percentage (percentage of trips that occurred in the busiest hour)
    val peakPercentage: Double = tripsByHourPct.agg(F.max("percentage").as("max_pct"))
      .select("max_pct").as[Double].collect().headOption.getOrElse(0.0)

    // Weekly total revenue
    val weeklyRevenue: Double = inputData.agg(F.sum(F.col("total_amount")).as("revenue"))
      .select("revenue").as[Double].collect().headOption.getOrElse(0.0)

    // Average revenue per mile (guard against zero or null distances)
    val avgRevenuePerMile: Double = inputData.filter(F.col("trip_distance") > 0)
      .agg(F.avg(F.col("total_amount") / F.col("trip_distance")).as("avg_rev_per_mile"))
      .select("avg_rev_per_mile").as[Double].collect().headOption.getOrElse(0.0)

    // Night trip count (define night as hours 0-5 inclusive)
    val nightTripCount: Long = withHour.filter(F.col("hour") >= 0 && F.col("hour") < 6).count()

    // We don't yet have a borough lookup (dimensional join), and avg_trip_time_vs_distance isn't defined.
    // For now: set weekly_trip_volume_by_borough to totalTrips and leave avg_trip_time_vs_distance as 0.
    val weeklyTripVolumeByBorough: Long = totalTrips
    val avgTripTimeVsDistance: Long = 0L

    Seq(ProjectKPIs(
      per_hour_trip_percentage = peakPercentage,
      weekly_trip_volume_by_borough = weeklyTripVolumeByBorough,
      avg_trip_time_vs_distance = avgTripTimeVsDistance,
      weekly_total_trips_and_total_revenue = weeklyRevenue,
      avg_revenue_per_mile = avgRevenuePerMile,
      night_trip_percentage = nightTripCount
    )).toDS()
  }

  def saveOutput(kpis: org.apache.spark.sql.Dataset[ProjectKPIs], outputPath: String): Unit = {
    kpis.write
      .mode("overwrite")
      .parquet(outputPath)
  }
}
