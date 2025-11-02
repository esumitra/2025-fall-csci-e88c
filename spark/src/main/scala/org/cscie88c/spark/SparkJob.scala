package org.cscie88c.spark

import org.cscie88c.core.Utils
// import org.cscie88c.BronzeDataIngestion
import org.apache.spark.sql.{DataFrame, SparkSession, Dataset}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

case class YellowTripSchema(
    // These are all headers in the trip parquet file
    // VendorID: String,
    tpep_pickup_datetime: String, // Required
    tpep_dropoff_datetime: String, // Required
    // passenger_count: Int,
    trip_distance: Double, // Required
    // RatecodeID: String,
    // store_and_fwd_flag: Boolean,
    PULocationID: Int, // Required
    DOLocationID: Int, // Required
    // payment_type: Int,
    fare_amount: Double //, // Required
    // extra: Double,
    // mta_tax: Double,
    // tip_amount: Double,
    // tolls_amount: Double,
    // improvement_surcharge: Double,
    // total_amount: Double,
    // congestion_surcharge: Double,
    // Airport_fee: Double,
    // cbd_congestion_fee: Double
)

case class test (
    tpep_pickup_datetime: String, // Required
    tpep_dropoff_datetime: String, // Required
    trip_distance: Double, // Required
    PULocationID: Int, // Required
    DOLocationID: Int, // Required
    fare_amount: Double // Required
)

// Following values are from the taxi_zone_lookup.csv

case class TaxiZoneSchema (
    LocationID: Int,
    Borough: String,
    Zone: Int,
    Service_zone: String
    )

object SparkJob {
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

      val filePath = "../data/bronze/yellow_tripdata_2025-01.parquet"
      val filePath2 = "../data/bronze/taxi_zone_lookup.csv"

      val YellowTrip_DF: Dataset[YellowTripSchema] = loadParquetFile(filePath)
      // val TaxiZone_DF: Dataset[TaxiZoneSchema] = loadCSVFile(filePath2)

      // Test Section
      spark.read.parquet(filePath).printSchema()

      println(s"Schema for $filePath:")
      YellowTrip_DF.show(5, truncate = false)  // Sample a few rows to inspect

      // println(s"Schema for $filePath2:")
      // TaxiZone_DF.show(5, truncate = false)  // Sample a few rows to inspect

      // println("Start Write")
      // YellowTrip_DF.write.mode("overwrite").option("header", "true").csv("output.csv")
      // println("Stop Write")

      spark.stop()
  }

  def loadParquetFile(filePath: String)(implicit spark: SparkSession): Dataset[YellowTripSchema] = {
      import spark.implicits._

      // Get field names from schema with a sequence
      val columns: Seq[String] = Seq(
      "tpep_pickup_datetime",
      "tpep_dropoff_datetime",
      "trip_distance",
      "PULocationID",
      "DOLocationID",
      "fare_amount"
      )

      spark.read
      .format("parquet")
      .option("header", "true")
      //       .option("inferSchema", "true")
      .parquet(filePath)
      .select(columns.map(col): _*) // Selects only needed columns defined above
      //       .selectExpr(columns: _*)
      // .select("tpep_pickup_datetime",
      //         "tpep_dropoff_datetime",
      //         "trip_distance",
      //         "PULocationID",
      //         "DOLocationID",
      //         "fare_amount"
      //         )
      .as[YellowTripSchema]
  }

  def loadCSVFile(filePath: String)(implicit spark: SparkSession): Dataset[TaxiZoneSchema] = {
      import spark.implicits._
  
      spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(filePath)
      .as[TaxiZoneSchema]
  }
}




// object BronzeDataIngestion {

//   def main(args: Array[String]): Unit = {
//       val Array(infile, outpath) = args
//       implicit val spark = SparkSession.builder()
//               .appName("BronzeDataIngestion")
//               .master("local[*]")
//               .getOrCreate()

//       val filePath = "../data/bronze/yellow_tripdata_2025-01.parquet"
//       val filePath2 = "../data/bronze/taxi_zone_lookup.csv"

//       val YellowTrip_DF: Dataset[YellowTripSchema] = loadParquetFile(filePath)
//       val TaxiZone_DF: Dataset[TaxiZoneSchema] = loadCSVFile(filePath2)

//       // Test Section
//       spark.read.parquet(filePath).printSchema()

//       println(s"Schema for $filePath:")
//       YellowTrip_DF.show(5, truncate = false)  // Sample a few rows to inspect

//       println(s"Schema for $filePath2:")
//       TaxiZone_DF.show(5, truncate = false)  // Sample a few rows to inspect

//       // println("Start Write")
//       // YellowTrip_DF.write.mode("overwrite").option("header", "true").csv("output.csv")
//       // println("Stop Write")

//       spark.stop()
//   }

//   def loadParquetFile(filePath: String)(implicit spark: SparkSession): Dataset[YellowTripSchema] = {
//       import spark.implicits._

//       // Get field names from schema with a sequence
//       val columns: Seq[String] = Seq(
//       "tpep_pickup_datetime",
//       "tpep_dropoff_datetime",
//       "trip_distance",
//       "PULocationID",
//       "DOLocationID",
//       "fare_amount"
//       )

//       spark.read
//       .format("parquet")
//       .option("header", "true")
//       //       .option("inferSchema", "true")
//       .parquet(filePath)
//       .select(columns.map(col): _*) // Selects only needed columns defined above
//       //       .selectExpr(columns: _*)
//       // .select("tpep_pickup_datetime",
//       //         "tpep_dropoff_datetime",
//       //         "trip_distance",
//       //         "PULocationID",
//       //         "DOLocationID",
//       //         "fare_amount"
//       //         )
//       .as[YellowTripSchema]
//   }

//   def loadCSVFile(filePath: String)(implicit spark: SparkSession): Dataset[TaxiZoneSchema] = {
//       import spark.implicits._
  
//       spark.read
//       .format("csv")
//       .option("header", "true")
//       .option("inferSchema", "true")
//       .csv(filePath)
//       .as[TaxiZoneSchema]
//   }

  // def main(args: Array[String]): Unit = {
  //   val spark = SparkSession.builder()
  //     .appName("BronzeDataReader")
  //     .master("local[*]")
  //     .getOrCreate()

  //   import spark.implicits._

  //   val filePath = "../data/bronze/yellow_tripdata_2025-01.parquet"
  //   val filePath2 = "../data/bronze/taxi_zone_lookup.csv"

  //   val YellowTrip_df = spark.read.parquet(filePath)
  //   val TaxiZone_df = spark.read.csv(filePath2)

  //   // Test Section
  //   println(s"Schema for $filePath:")
  //   YellowTrip_df.show(10, truncate = false)  // Sample a few rows to inspect

  //   println(s"Schema for $filePath2:")
  //   TaxiZone_df.show(10, truncate = false)  // Sample a few rows to inspect


  //   val YellowTrip_DF: Dataset[YellowTripSchema] = BronzeDataIngestion.loadParquetFile(filePath)
  //   val TaxiZone_DF: Dataset[TaxiZoneSchema] = BronzeDataIngestion.loadCSVFile(filePath2)

  //   // Test Section
  //   spark.read.parquet(filePath).printSchema()

  //   println(s"Schema for $filePath:")
  //   YellowTrip_DF.show(10, truncate = false)  // Sample a few rows to inspect

  //   println(s"Schema for $filePath2:")
  //   TaxiZone_DF.show(10, truncate = false)  // Sample a few rows to inspect

  //   spark.stop()
  // }
// }
