package org.cscie88c.spark

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

import java.time.format.DateTimeFormatter


object GoldJob {


  val GoldRoot = "/opt/spark-data/gold"


  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("GoldJob-KPI-Compute")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")


    processKPIS(GoldRoot)(spark)

    spark.stop()
  }

  def processKPIS(goldRoot: String)(spark:SparkSession) = {
    import spark.implicits._
    println("\n=== GOLD JOB: READING SILVER CONFORMED DATA ===")
    val silverDF = spark.read.parquet(Utilities.SilverTripsConformed)
    println(s"Loaded ${silverDF.count()} rows from Silver.")

    // ================================================================
    // KPI 1 — Weekly Trip Volume by Borough
    // ================================================================
    val weeklyTripVolume = silverDF
      .groupBy("pickup_week_start", "pickup_borough")
      .agg(count("*").as("trip_volume"))
      .orderBy("pickup_week_start", "pickup_borough")

    // ================================================================
    // KPI 2 — Peak Hour Trip Percentage (7–9 AM & 5–7 PM)
    // ================================================================
    val peakTrips:Double = silverDF.filter(
      ($"pickup_hour".between(7, 9)) || ($"pickup_hour".between(17, 19))
    ).count().toDouble

    val totalTrips = silverDF.count().toDouble
    val peakHourPct = (peakTrips / totalTrips) * 100
    val peakHourPctFormatted = f"$peakHourPct%.2f%%"

    // ================================================================
    // KPI 3 — Avg Trip Time vs Distance per Week
    // ================================================================
    val avgTimeVsDistance = silverDF
      .groupBy("pickup_week_start")
      .agg(
        avg("trip_duration_min").as("avg_duration_min"),
        avg("trip_distance").as("avg_distance_miles")
      )
      .orderBy("pickup_week_start")

    // ================================================================
    // KPI 4 — Weekly Trips & Weekly Revenue
    // ================================================================
    val weeklyTripsRevenue = silverDF
      .groupBy("pickup_week_start")
      .agg(
        count("*").as("total_trips"),
        sum("total_amount").as("total_revenue")
      )
      .orderBy("pickup_week_start")

    // ================================================================
    // KPI 5 — Avg Revenue per Mile
    // ================================================================
    val totalRevenue = silverDF.agg(sum("total_amount")).as[Double].first()
    val totalMiles = silverDF.agg(sum("trip_distance")).as[Double].first()
    val avgRevenuePerMile =
      if (totalMiles > 0) totalRevenue / totalMiles else 0.0

    // ================================================================
    // KPI 6 — Night Trip Percentage (10 PM – 4 AM)
    // ================================================================
    val nightTrips:Double = silverDF.filter(
      ($"pickup_hour" >= 22) || ($"pickup_hour" <= 4)
    ).count()

    val nightPct = (nightTrips / totalTrips) * 100

    // ================================================================
    // === READABLE DATA PREVIEW (the part you asked to restore) ===
    // ================================================================
    println("\n=== Weekly Trip Volume by Borough ===")
    weeklyTripVolume.show(10, truncate = false)

    println(s"\n✅ Peak Hour Trip Percentage: $peakHourPctFormatted")

    println("\n=== Average Trip Time vs Distance ===")
    avgTimeVsDistance.show(10, truncate = false)

    println("\n=== Weekly Trips & Revenue ===")
    weeklyTripsRevenue.show(10, truncate = false)

    println(f"\n✅ Avg Revenue per Mile: $$$avgRevenuePerMile%.2f")
    println(f"✅ Night Trip Percentage: ${nightPct}%.2f%%")

    // ================================================================
    // Step — Create a new run folder for validation
    // ================================================================
    val runId = java.time.LocalDateTime.now()
      .format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"))

    val runPath = s"$goldRoot/kpis/run_$runId"
    println(s"\n=== SAVING GOLD OUTPUTS TO: $runPath ===")

    // ================================================================
    // Save KPI outputs
    // ================================================================
    weeklyTripVolume.write.mode("overwrite").parquet(s"$runPath/weekly_trip_volume")
    println(s"✅ Saved: $runPath/weekly_trip_volume")

    weeklyTripsRevenue.write.mode("overwrite").parquet(s"$runPath/weekly_trips_revenue")
    println(s"✅ Saved: $runPath/weekly_trips_revenue")

    avgTimeVsDistance.write.mode("overwrite").parquet(s"$runPath/time_vs_distance")
    println(s"✅ Saved: $runPath/time_vs_distance")

    // Summary metrics for quick UI access
    val summaryDF: DataFrame = Seq(
      ("peak_hour_pct", peakHourPct),
      ("avg_revenue_per_mile", avgRevenuePerMile),
      ("night_trip_pct", nightPct)
    ).toDF("metric", "value")

    Utilities.parquetOutput(summaryDF,s"$runPath/kpi_summary")
    summaryDF.write.mode("overwrite").parquet(s"$runPath/kpi_summary")
    println(s"✅ Saved summary KPIs: $runPath/kpi_summary")

    // ================================================================
    // Evidence-compatible CSV exports
    // ================================================================
    println("\n=== EXPORTING DATA FOR EVIDENCE VISUALIZATION ===")
    val evidencePath = s"$goldRoot/evidence"
    
    // Export KPIs as CSVs for Evidence
    weeklyTripVolume
      .coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv(s"$evidencePath/weekly_trip_volume")
    
    weeklyTripsRevenue
      .coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv(s"$evidencePath/weekly_trips_revenue")
      
    avgTimeVsDistance
      .coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv(s"$evidencePath/time_vs_distance")
      
    summaryDF
      .coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv(s"$evidencePath/kpi_summary")
    
    println(s"✅ Evidence CSV files exported to: $evidencePath")

    println("\n=== GOLD JOB COMPLETE ✅ ===")
  }

}
