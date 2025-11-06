package org.cscie88c.spark

import org.cscie88c.spark.{YellowTripSchema, TaxiZoneSchema}
import org.cscie88c.{RunSummary, BronzeDataIngestion, DataQualityChecks, SilverFunctions, AnalysisFunctions}
import org.apache.spark.sql.{SparkSession, Dataset}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{hour}

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
              .appName("Batch Data Pipeline")
              .master("local[*]")
              .getOrCreate()
      
      import spark.implicits._

      // Moved to top to reduce logging for now
      spark.sparkContext.setLogLevel("ERROR") // Show reduce log bloat for testing

      // Bronze Data Section 
      val yellowTripDataFilePath = "../data/bronze/yellow_tripdata_2025-01.parquet"
      val taxiZoneLookupFilePath = "../data/bronze/taxi_zone_lookup.csv"

      val tripsDS: Dataset[YellowTripSchema] =  BronzeDataIngestion.loadParquetFile(yellowTripDataFilePath)
      val zonesDS: Dataset[TaxiZoneSchema] = BronzeDataIngestion.loadCSVFile(taxiZoneLookupFilePath)

      // Bronze Testing Section ( Will need to be moved to a test file )

      spark.read.parquet(yellowTripDataFilePath).printSchema()

      println(s"How many records in the parquet file: ${tripsDS.count()}")

      println(s"Schema for $yellowTripDataFilePath:")
      tripsDS.show(5, truncate = false)  // Sample a few rows to inspect

      println(s"Schema for $taxiZoneLookupFilePath:")
      zonesDS.show(5, truncate = false)  // Sample a few rows to inspect

      // Create DataFrames
      // val tripsDF = tripsDS.toDF().alias("trips")

      // Add unique trip_id column to DF (for reference later)
      val tripsDF = tripsDS
        .toDF()                  // Convert to DataFrame if not already
        .withColumn("trip_id", monotonically_increasing_id()) // This will give us a unique ID to key off of, sequentially increasing
        .alias("trips")
      val zonesDF = zonesDS.toDF().alias("zones")  

      println("=== Null Check ===")
      DataQualityChecks.nullPercentages(tripsDF).show(false)

      println("=== Range Check ===")
      DataQualityChecks.rangeChecks(tripsDF).show(false)

      println("=== Unique ID Check ===")
      DataQualityChecks.uniqueIdCheck(tripsDF, "trip_id").show(false)

      // println("=== Week Completeness Check ===")
      // DataQualityChecks.completenessByWeek(tripsDF).show(false)

      // println("=== Referential Integrity Check ===")
      // DataQualityChecks.referentialCheck(tripsDS, zonesDS).show(false)

      println("--- TEST RUN SUMMARY START HERE ---")
      val summaryDF = RunSummary.generateSummary(tripsDF)
      summaryDF.show(false)
      summaryDF.write.mode("overwrite")
        .option("header", "true")
        .csv("../data/bronze/summary.csv")
      println("--- TEST RUN SUMMARY END HERE ---")


      
      // joined/linked table will need DataFrames, then create based off PU or DO    
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
        col("PU.trip_distance").as("trip_distance"),
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
        .withColumn("Trip_Time_Min_Converted", col("Trip_Time").cast("double") / 60) // Converted from sec to min 

        // Chi's Code from Announcement
        // new columns for Part B requirements
        // Peak-Hour Trip %
        .withColumn("Is_Peak_Hour", when(col("Pickup_Hour").between(7,9) // Morning Peak (7-9) = 1
          .or(col("Pickup_Hour").between(16,18)), 1).otherwise(0)) // Evening Peak (16-18) = 1 otherwise 0
        // Weekly Trip Volume by Borough
        .withColumn("Week_Borough", concat_ws("_", col("Pickup_Week"), col("BoroughPU"))) // get week and borough e.g., "1_Manhattan"  
        // Avg Trip Time vs Distance
        .withColumn("Avg_Trip_Speed_Mph", when(col("Trip_Time_Min_Converted") > 0, 
          (col("trip_distance") / col("Trip_Time_Min_Converted")) * 60).otherwise(0)) // distance / time (in hours) in mph
        // Total Trips & Total Revenue (Weekly)
        .withColumn("Week_Borough_Revenue", concat_ws("_", col("Pickup_Week"), col("BoroughPU"))) // get week and borough e.g., "1_Manhattan"
        // Avg Revenue per Mile
        .withColumn("Revenue_per_Mile", when(col("trip_distance") > 0,
          col("fare_amount") / col("trip_distance")).otherwise(0)) // fare amount / distance
        // Night Trip %
        .withColumn("Is_Night_Trip", when(col("Pickup_Hour").between(21,23) // Nighttime (9PM-11PM) = 1
          .or(col("Pickup_Hour").between(0,3)), 1).otherwise(0)) // Early Morning (12AM-3AM) = 1 otherwise 0


      // Silver Layer Cleaning
      val cleanedDF = combinedDF
        .transform(SilverFunctions.validTimeOrder)
        .transform(SilverFunctions.validFareAmount)
        .transform(SilverFunctions.validTripDistance)


      // Review Section

      // println("--- CombinedDF Review ---")
      // combinedDF.show(5, truncate = false)
      // println("--- End CombinedDF Review ---")

      println("--- CleanedDF Review ---")
      cleanedDF.show(5, truncate = false)
      println("--- End CombinedDF Review ---")

      // Second Summary to confirm records changed and data is cleaned
      println("--- TEST RUN 2ND SUMMARY START HERE ---")
      val summaryCleanedDF = RunSummary.generateSummary(cleanedDF)
      summaryCleanedDF.show(false)
      summaryCleanedDF.write.mode("overwrite")
        .option("header", "true")
        .csv("../data/silver/summary_cleaned.csv")
      println("--- TEST RUN 2ND SUMMARY END HERE ---")

      // What do we need to do with this atm?
      val week1 = SilverFunctions.getWeekData(combinedDF, 1)
      val week2 = SilverFunctions.getWeekData(combinedDF, 2)
      val week3 = SilverFunctions.getWeekData(combinedDF, 3)
      val week4 = SilverFunctions.getWeekData(combinedDF, 4)
      val week5 = SilverFunctions.getWeekData(combinedDF, 5)

      // Block Review
      println("--- Weeks 1-5 Review ---")
      week1.show(2, truncate = false)
      week2.show(2, truncate = false)
      week3.show(2, truncate = false)
      week4.show(2, truncate = false)
      week5.show(2, truncate = false)
      println("--- End Weeks 1-5 Review ---")

      val boroughVol = combinedDF
        .join(
          zonesDF, 
          combinedDF("LocationIDPU") === zonesDF("LocationID"),
          "left")
        .groupBy("Borough", "Pickup_Week")
        .count()
        .withColumnRenamed("count", "TripVolume")
        .orderBy("Borough", "Pickup_Week")

      // For testing uncomment this
      println("--- Borough Volume Review ---")
      boroughVol.show(5, truncate = false)
      println("--- End Borough Volume Review ---")

      val tripTimePerDistance = combinedDF
        .withColumn("TimePerDistance", col("Trip_Time_Min_Converted") / col("trip_distance"))
        .filter(col("trip_distance") > 0) // Avoid divide by zero error
        .join(
          zonesDF.select(
            col("LocationID").alias("PULocation"),
            col("Zone").alias("PUZone"),
            col("Borough").alias("PUBorough")
          ),
          Seq("PULocation"),
          "left"
        )
        .join(
          zonesDF.select(
            col("LocationID").alias("DOLocation"),
            col("Zone").alias("DOZone"),
            col("Borough").alias("DOBorough")
          ),
          Seq("DOLocation"),
          "left"
        )
        .groupBy("PUZone", "DOZone", "Pickup_Week")
        .agg(avg("TimePerDistance").alias("AvgTimePerDistance"))
        .orderBy(desc("AvgTimePerDistance"))

        println("--- Trip Time Per Distance Review ---")
        tripTimePerDistance.show(10, truncate = false)
        println("--- End Trip Time Per Distance Review ---")


      // Test Section
  
      val (borough1, peakPercentage) = AnalysisFunctions.getPeakHourTripPercentage(combinedDF, "Manhattan")
      println(s"1. Peak-Hour Trip Percentage for $borough1: $peakPercentage")

        // 2. Weekly Trip Volume by Borough (e.g., input = "Manhattan")
      //val (borough2, tripVolume) = AnalysisFunctions.getWeeklyTripVolumeByBorough(combinedDF, "Manhattan")
      //  println(s"2. Total Trip Volume for $borough2 (Dataset Period): $tripVolume")

        // Alternative for weekly trip volume by borough
      val tripVolumeByWeekDF = AnalysisFunctions.getTripVolumeByWeekForAllBoroughs(combinedDF, 1)
      tripVolumeByWeekDF.show(false)

        // 3. Total Trips & Total Revenue (Weekly) (e.g., input week = 1, Manhattan)
      AnalysisFunctions.getTotalTripsAndRevenueByWeek(combinedDF, 1, "Manhattan").show(false)

        // 4. Compare efficiency across boroughs (Avg Revenue per Mile)
      AnalysisFunctions.compareEfficiencyAcrossBoroughs(combinedDF).show(false)
        // 5. Night Trip Percentage for a Borough (e.g., input = "Manhattan")
      val (borough3, nightPercentage) = AnalysisFunctions.getNightTripPercentage(combinedDF , "Manhattan")
      println(s"5. Night Trip Percentage for $borough3: $nightPercentage")

      println("=== END NEW TEST ===\n")

      println(s"Schema for JOINED table:")
      ComboYellowTripTaxiZonesDF.show(5, truncate = false)  // Sample a few rows to inspect

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
