package org.cscie88c.spark

// Copied the StandardTest file imports
// import org.scalatest.matchers.should.Matchers
// import org.scalatest.wordspec.AnyWordSpec
// import org.scalatest.BeforeAndAfterAll

import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.{SparkSession}
import org.apache.spark.sql.functions._

class BronzeTests extends AnyFunSuite {

  // Create a local SparkSession for testing
  val spark: SparkSession = SparkSession.builder()
    .appName("BronzeTests")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  // File paths
  val yellowTripDataFilePath = "../data/bronze/yellow_tripdata_2025-01.parquet"
  val taxiZoneLookupFilePath = "../data/bronze/taxi_zone_lookup.csv"

  // Basic non-empty test, followed by checking column headers
  test("load yellow trip parquet file") {
    val tripsDF = spark.read.parquet(yellowTripDataFilePath)
    assert(tripsDF.count() > 0)
    
    val expectedCols = Seq(
      "tpep_pickup_datetime",
      "tpep_dropoff_datetime",
      "trip_distance",
      "PULocationID",
      "DOLocationID",
      "fare_amount"
    )
    expectedCols.foreach(col => assert(tripsDF.columns.contains(col)))
  }

  // Tests to create

  // Value ranges
  // 

  test("pickup/dropoff zones exist in lookup") {
    val tripsDF = spark.read.parquet(yellowTripDataFilePath)
    val zonesDF = spark.read.option("header", "true").csv(taxiZoneLookupFilePath)

    val invalidPU = tripsDF.join(zonesDF, tripsDF("PULocationID") === zonesDF("LocationID"), "left_anti").count()
    val invalidDO = tripsDF.join(zonesDF, tripsDF("DOLocationID") === zonesDF("LocationID"), "left_anti").count()

    assert(invalidPU == 0)
    assert(invalidDO == 0)
  }

  // Stop Spark after all tests
  def stopSpark(): Unit = spark.stop()
}

