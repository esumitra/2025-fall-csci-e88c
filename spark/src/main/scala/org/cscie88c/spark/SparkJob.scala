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

      val trips: Dataset[YellowTripSchema] =  BronzeDataIngestion.loadParquetFile(yellowTripDataFilePath)
      val zones: Dataset[TaxiZoneSchema] = BronzeDataIngestion.loadCSVFile(taxiZoneLookupFilePath) // Erroring atm!


      // Test Section
      spark.sparkContext.setLogLevel("ERROR") // Show reduce log bloat for testing

      spark.read.parquet(yellowTripDataFilePath).printSchema()

      println(s"How many records in the parquet file: ${trips.count()}")


      println(s"Schema for $yellowTripDataFilePath:")
      trips.show(15, truncate = false)  // Sample a few rows to inspect

      println(s"Schema for $taxiZoneLookupFilePath:")
      zones.show(5, truncate = false)  // Sample a few rows to inspect

      // This generated a folder called output.csv with segmented files
      // trips.write.mode("overwrite").option("header", "true").csv("output.csv")

      println("=== Null Check ===")
      DataQualityChecks.nullPercentages(trips.toDF()).show(false)

      println("=== Range Check ===")
      DataQualityChecks.rangeChecks(trips).show(false)

      println("=== Referential Integrity Check ===")
      // DataQualityChecks.referentialCheck(trips, zones).show(false)

      spark.stop()
  }
}
