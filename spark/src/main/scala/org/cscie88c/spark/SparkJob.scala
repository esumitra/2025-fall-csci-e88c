package org.cscie88c.spark

import org.cscie88c.core.Utils
import org.cscie88c.spark.YellowTripSchema
import org.cscie88c.spark.TaxiZoneSchema
import org.cscie88c.BronzeDataIngestion
import org.apache.spark.sql.{DataFrame, SparkSession, Dataset}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

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

      val YellowTrip_DF: Dataset[YellowTripSchema] =  BronzeDataIngestion.loadParquetFile(filePath)
      // val TaxiZone_DF: Dataset[TaxiZoneSchema] = BronzeDataIngestion.loadCSVFile(filePath2) // Erroring atm

      // Test Section
      spark.read.parquet(filePath).printSchema()

      println(s"Schema for $filePath:")
      YellowTrip_DF.show(15, truncate = false)  // Sample a few rows to inspect

      // println(s"Schema for $filePath2:")
      // TaxiZone_DF.show(5, truncate = false)  // Sample a few rows to inspect

      // println("Start Write")
      // YellowTrip_DF.write.mode("overwrite").option("header", "true").csv("output.csv")
      // println("Stop Write")

      spark.stop()
  }

  // def loadParquetFile(filePath: String)(implicit spark: SparkSession): Dataset[YellowTripSchema] = {
  //     import spark.implicits._

  //     // Get field names from schema with a sequence
  //     val columns: Seq[String] = Seq(
  //     "tpep_pickup_datetime",
  //     "tpep_dropoff_datetime",
  //     "trip_distance",
  //     "PULocationID",
  //     "DOLocationID",
  //     "fare_amount"
  //     )

  //     spark.read
  //     .format("parquet")
  //     .option("header", "true")
  //     //       .option("inferSchema", "true")
  //     .parquet(filePath)
  //     .select(columns.map(col): _*) // Selects only needed columns defined above
  //     //       .selectExpr(columns: _*)
  //     // .select("tpep_pickup_datetime",
  //     //         "tpep_dropoff_datetime",
  //     //         "trip_distance",
  //     //         "PULocationID",
  //     //         "DOLocationID",
  //     //         "fare_amount"
  //     //         )
  //     .as[YellowTripSchema]
  // }

  // def loadCSVFile(filePath: String)(implicit spark: SparkSession): Dataset[TaxiZoneSchema] = {
  //     import spark.implicits._
  
  //     spark.read
  //     .format("csv")
  //     .option("header", "true")
  //     .option("inferSchema", "true")
  //     .csv(filePath)
  //     .as[TaxiZoneSchema]
  // }
}
