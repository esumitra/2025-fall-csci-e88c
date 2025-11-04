package org.cscie88c.spark

import org.cscie88c.core.Utils
import org.cscie88c.spark.{YellowTripSchema, TaxiZoneSchema} // Greg's Files
import org.cscie88c.{RunSummary, BronzeDataIngestion, DataQualityChecks, SilverFunctions} // Greg's Files
import org.apache.spark.sql.{DataFrame, SparkSession, Dataset}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{from_unixtime, hour, to_date, datediff}

object SparkJob {
  // Original scaffolding code

  // def main(args: Array[String]): Unit = {
  //   val spark = SparkSession.builder()
  //     .appName("SampleSparkJob")
  //     .master("local[*]")
  //     .getOrCreate()

  //   import spark.implicits._

  //   val df = Seq("Alice", "Bob", "Carol").toDF("name")
  //   val greeted = df.map(row => Utils.greet(row.getString(0)))

  //   greeted.show(false)
  //   spark.stop()
  // }

  def main(args: Array[String]): Unit = {
      val Array(infile, outpath) = args
      implicit val spark = SparkSession.builder()
              .appName("BronzeDataIngestion")
              .master("local[*]")
              .getOrCreate()
      
      import spark.implicits._

      val yellowTripDataFilePath = "../data/bronze/yellow_tripdata_2025-01.parquet"
      val taxiZoneLookupFilePath = "../data/bronze/taxi_zone_lookup.csv"

      val tripsDS: Dataset[YellowTripSchema] =  BronzeDataIngestion.loadParquetFile(yellowTripDataFilePath)
      val zonesDS: Dataset[TaxiZoneSchema] = BronzeDataIngestion.loadCSVFile(taxiZoneLookupFilePath)
      
      // joined/linked table will need DataFrames, then create based off PU or DO
      val tripsDF = tripsDS.toDF().alias("trips")
      val zonesDF = zonesDS.toDF().alias("zones")      
      val joinedTripsZonesDFPU = tripsDF.join(
        zonesDF,
        tripsDF("PULocationID") === zonesDF("LocationID"),
        "inner"
      )
      val joinedTripsZonesDFDO = tripsDF.join(
        zonesDF,
        tripsDF("DOLocationID") === zonesDF("LocationID"),
        "inner"
      )

      // joined table from PU and DO zone information incorporated
      val joinedTripsZonesDFRequirements = (
        ((joinedTripsZonesDFDO("tpep_pickup_datetime")) === (joinedTripsZonesDFPU("tpep_pickup_datetime"))) &&
        ((joinedTripsZonesDFDO("tpep_dropoff_datetime")) === (joinedTripsZonesDFPU("tpep_dropoff_datetime"))) &&
        ((joinedTripsZonesDFDO("trip_distance")) === (joinedTripsZonesDFPU("trip_distance"))) &&
        ((joinedTripsZonesDFDO("PULocationID")) === (joinedTripsZonesDFPU("PULocationID"))) &&
        ((joinedTripsZonesDFDO("DOLocationID")) === (joinedTripsZonesDFPU("DOLocationID"))) &&
        ((joinedTripsZonesDFDO("fare_amount")) === (joinedTripsZonesDFPU("fare_amount")))
      )
      val joinedTripsZonesDF = joinedTripsZonesDFDO.alias("DO").join(
        joinedTripsZonesDFPU.alias("PU"),
        joinedTripsZonesDFRequirements,
        "inner"
      )

      // create a table without duplicate rows from the joined table
      val ComboYellowTripTaxiZonesDF = joinedTripsZonesDF.select(
        col("PU.tpep_pickup_datetime").as("tpep_pickup_datetime"),
        col("PU.tpep_dropoff_datetime").as("tpep_dropoff_datetime"),
        col("PU.trip_distance").as("distance"),
        col("PU.PULocationID").as("PULocation"),
        col("PU.LocationID").as("LocationIDPU"),
        col("PU.Borough").as("BoroughPU"),
        col("PU.Zone").as("ZonePU"),
        col("PU.Service_zone").as("Service_zonePU"),
        col("DO.DOLocationID").as("DOLocation"),
        col("DO.LocationID").as("LocationIDDO"),
        col("DO.Borough").as("BoroughDO"),
        col("DO.Zone").as("ZoneDO"),
        col("DO.Service_zone").as("Service_zoneDO"),
        col("PU.fare_amount").as("fare_amount")
      )

      val combinedDF = ComboYellowTripTaxiZonesDF
        .withColumn("Pickup_Hour", hour(col("tpep_pickup_datetime")))
        .withColumn("Pickup_Week", weekofyear(col("tpep_pickup_datetime")))
        .withColumn("Trip_Time", unix_timestamp(to_timestamp($"tpep_dropoff_datetime")) - unix_timestamp(to_timestamp($"tpep_pickup_datetime")))
        .withColumn("Trip_Time_Min_Converted", col("Trip_Time").as[Double] / 60) // Converted from sec to min

      combinedDF.show(10, truncate = false)

      // Test Section
      spark.sparkContext.setLogLevel("ERROR") // Show reduce log bloat for testing

      spark.read.parquet(yellowTripDataFilePath).printSchema()

      println(s"How many records in the parquet file: ${tripsDS.count()}")

      println(s"Schema for $yellowTripDataFilePath:")
      tripsDS.show(5, truncate = false)  // Sample a few rows to inspect

      println(s"Schema for $taxiZoneLookupFilePath:")
      zonesDS.show(5, truncate = false)  // Sample a few rows to inspect

      println(s"Schema for JOINED table:")
      ComboYellowTripTaxiZonesDF.show(5, truncate = false)  // Sample a few rows to inspect

      println("=== Null Check ===")
      DataQualityChecks.nullPercentages(tripsDS.toDF()).show(false)

      println("=== Range Check ===")
      DataQualityChecks.rangeChecks(tripsDS).show(false)

      println("=== Referential Integrity Check ===")
      DataQualityChecks.referentialCheck(tripsDS, zonesDS).show(false)

      println("--- TEST RUN SUMMARY START HERE ---")
        val summaryDF = RunSummary.generateSummary(tripsDS, zonesDS)
        summaryDF.show(false)
        summaryDF.write.mode("overwrite")
            .option("header", "true")
            .csv("../data/bronze/summary.csv")
        println("--- TEST RUN SUMMARY END HERE ---")

      spark.stop()
  }
}

/*

--- TO DO ---

Part B

Create reusable functions for week bucketing and other data transformations.

  - Peak-Hour Trip %: Identify congestion trends across boroughs.
  - Weekly Trip Volume by Borough: Track mobility demand trends and compare boroughs over time
  - Avg Trip Time vs Distance: Detecting congestion clusters and route inefficiencies.

  - Total Trips & Total Revenue (Weekly): Monitor revenue growth and trip counts
  - Avg Revenue per Mile: Compare efficiency across boroughs
  - Night Trip %: Balance driver scheduling and safety coverage (9PM-3AM)

Part C

Implement weekly aggregations, anomaly detection, and unit tests against small fixtures.

Write validation comparing two runs (e.g., prior week) and produce csv or parquet files.

Part D

sbt setup, scalafmt, diagrams, demo notebook/script.

Owns the final results and ensures everyone’s part integrates.

Owns visualization of KPI dashboard


Bonus

Run the spark job on GCP Dataproc or AWS EMR

*/
