package org.cscie88c

import org.apache.spark.sql.{DataFrame, SparkSession, Dataset}
import org.cscie88c.spark.YellowTripSchema
import org.cscie88c.spark.TaxiZoneSchema

object BronzeDataIngestion {

    def main(args: Array[String]): Unit = {
        val Array(infile, outpath) = args
        implicit val spark = SparkSession.builder()
        .appName("BronzeDataIngestion")
        .master("local[*]")
        .getOrCreate()

        val filePath = "../data/bronze/yellow_tripdata_2025-01.parquet"
        val filePath2 = "../data/bronze/taxi_zone_lookup.csv"

        val YellowTrip_DF: Dataset[YellowTripSchema] = loadParquetFile(filePath)
        val TaxiZone_DF: Dataset[TaxiZoneSchema] = loadCSVFile(filePath2)

        // Test Section
        println(s"Schema for $filePath:")
        YellowTrip_DF.show(5, truncate = false)  // Sample a few rows to inspect

        println(s"Schema for $filePath2:")
        TaxiZone_DF.show(5, truncate = false)  // Sample a few rows to inspect


        spark.stop()
    }

    def loadParquetFile(filePath: String)(implicit spark: SparkSession): Dataset[YellowTripSchema] = {
    import spark.implicits._
    
    spark.read
      .format("parquet")
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(filePath)
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

/*
import spark.implicits._

        val filePath = "../data/bronze/yellow_tripdata_2025-01.parquet"
        val filePath2 = "../data/bronze/taxi_zone_lookup.csv"

        val YellowTrip_df = spark
                .read
                .format("parquet")
                .option("header","true")
                .option("inferSchema","true")
                .load(filePath)

        val TaxiZone_df = spark
                .read
                .format("csv")
                .option("header","true")
                .option("inferSchema","true")
                .load(filePath2)

        // Test Section
        println(s"Schema for $filePath:")
        YellowTrip_df.show(5, truncate = false)  // Sample a few rows to inspect

        // println(s"Schema for $filePath2:")
        // TaxiZone_df.show(5, truncate = false)  // Sample a few rows to inspect

*/




/*
First output to review

|VendorID|tpep_pickup_datetime|tpep_dropoff_datetime|passenger_count|trip_distance|RatecodeID|store_and_fwd_flag|PULocationID|DOLocationID|payment_type|fare_amount|extra|mta_tax|tip_amount|tolls_amount|improvement_surcharge|total_amount|congestion_surcharge|Airport_fee|cbd_congestion_fee|
[info] +--------+--------------------+---------------------+---------------+-------------+----------+------------------+------------+------------+------------+-----------+-----+-------+----------+------------+---------------------+------------+--------------------+-----------+------------------+
[info] |1       |2025-01-01 00:18:38 |2025-01-01 00:26:59  |1              |1.6          |1         |N                 |229         |237         |1           |10.0       |3.5  |0.5    |3.0       |0.0         |1.0                  |18.0        |2.5                 |0.0        |0.0               |
[info] |1       |2025-01-01 00:32:40 |2025-01-01 00:35:13  |1              |0.5          |1         |N                 |236         |237         |1           |5.1        |3.5  |0.5    |2.02      |0.0         |1.0                  |12.12       |2.5                 |0.0        |0.0               |
[info] |1       |2025-01-01 00:44:04 |2025-01-01 00:46:01  |1              |0.6          |1         |N                 |141         |141         |1           |5.1        |3.5  |0.5    |2.0       |0.0         |1.0                  |12.1        |2.5                 |0.0        |0.0               |
[info] |2       |2025-01-01 00:14:27 |2025-01-01 00:20:01  |3              |0.52         |1         |N                 |244         |244         |2           |7.2        |1.0  |0.5    |0.0       |0.0         |1.0                  |9.7         |0.0                 |0.0        |0.0               |
[info] |2       |2025-01-01 00:21:34 |2025-01-01 00:25:06  |3              |0.66         |1         |N                 |244         |116         |2           |5.8        |1.0  |0.5    |0.0       |0.0         |1.0                  |8.3         |0.0                 |0.0        |0.0               |
[info] +--------+--------------------+---------------------+---------------+-------------+----------+------------------+------------+------------+------------+-----------+-----+-------+----------+------------+---------------------+------------+--------------------+-----------+------------------+

*/


