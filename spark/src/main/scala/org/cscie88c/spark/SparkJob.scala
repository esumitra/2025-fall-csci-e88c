package org.cscie88c.spark

import org.cscie88c.core.Utils
import org.cscie88c.spark.YellowTripSchema
import org.cscie88c.spark.TaxiZoneSchema
import org.cscie88c.BronzeDataIngestion
import org.cscie88c.DataQualityChecks
import org.apache.spark.sql.{DataFrame, SparkSession, Dataset}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

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

      // Test Section
      spark.sparkContext.setLogLevel("ERROR") // Show reduce log bloat for testing

      spark.read.parquet(yellowTripDataFilePath).printSchema()

      println(s"How many records in the parquet file: ${tripsDS.count()}")


      println(s"Schema for $yellowTripDataFilePath:")
      tripsDS.show(5, truncate = false)  // Sample a few rows to inspect

      println(s"Schema for $taxiZoneLookupFilePath:")
      zonesDS.show(5, truncate = false)  // Sample a few rows to inspect

      println(s"Schema for JOINED table:")
      ComboYellowTripTaxiZonesDF.show(20, truncate = false)  // Sample a few rows to inspect

      // This generated a folder called output.csv with segmented files
      // trips.write.mode("overwrite").option("header", "true").csv("output.csv")

      println("=== Null Check ===")
      DataQualityChecks.nullPercentages(tripsDS.toDF()).show(false)

      println("=== Range Check ===")
      DataQualityChecks.rangeChecks(tripsDS).show(false)

      println("=== Referential Integrity Check ===")
      // DataQualityChecks.referentialCheck(tripsDS, zonesDS).show(false)

      spark.stop()
  }
}
