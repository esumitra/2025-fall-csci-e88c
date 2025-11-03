package org.cscie88c

import org.apache.spark.sql.{DataFrame, SparkSession, Dataset}
import org.cscie88c.spark.{YellowTripSchema, TaxiZoneSchema}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object BronzeDataIngestion {

    // As suspected, SparkJob is the primary main, and we do not need this main here. Changes made to it don't show up when running SparkJob main

    // def main(args: Array[String]): Unit = {
    //     val Array(infile, outpath) = args
    //     implicit val spark = SparkSession.builder()
    //             .appName("BronzeDataIngestion")
    //             .master("local[*]")
    //             .getOrCreate()

    //     val filePath = "../data/bronze/yellow_tripdata_2025-01.parquet"
    //     val filePath2 = "../data/bronze/taxi_zone_lookup.csv"

    //     val YellowTrip_DF: Dataset[YellowTripSchema] = loadParquetFile(filePath)
    //     val TaxiZone_DF: Dataset[TaxiZoneSchema] = loadCSVFile(filePath2)

    //     // Test Section
    //     spark.read.parquet(filePath).printSchema()

    //     println(s"Schema for $filePath:")
    //     YellowTrip_DF.show(10, truncate = false)  // Sample a few rows to inspect

    //     println(s"Schema for $filePath2:")
    //     TaxiZone_DF.show(10, truncate = false)  // Sample a few rows to inspect


    //     spark.stop()
    // }

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
        // .option("inferSchema", "true") // Not sure if needed
        .parquet(filePath)
        .select(columns.map(col): _*) // Selects only needed columns defined above
        // Can alternatively be used to avoid mapping above
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
        // .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(filePath)
        .as[TaxiZoneSchema]
   }
}
