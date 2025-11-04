package org.cscie88c.spark

import org.apache.spark.sql.SparkSession
import org.cscie88c.core.Utils

object SparkJob {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("SampleSparkJob")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._


    val df = TaxiZones.zonesFromFile("/opt/spark-data/taxi_zone_lookup.csv")(spark)
    val pq = TripData.loadParquetData("/opt/spark-data/yellow_tripdata_2025-01.parquet")(spark)
    df.count()
    pq.count()
    df.show()
    pq.show(50)
    println("*********************************************")
    println("VendorID is null " + pq.where("VendorID IS NULL").count.toString + " times")
    println("tpep_pickup_datetime is null " + pq.where("tpep_pickup_datetime IS NULL").count.toString + " times")
    println("tpep_dropoff_datetime is null " + pq.where("tpep_dropoff_datetime IS NULL").count.toString + " times")
    println("passenger_count is null " + pq.where("passenger_count IS NULL").count.toString + " times")
    println("trip_distance is null " + pq.where("trip_distance IS NULL").count.toString + " times")
    println("RatecodeID is null " + pq.where("RatecodeID IS NULL").count.toString + " times")
    println("store_and_fwd_flag is null " + pq.where("store_and_fwd_flag IS NULL").count.toString + " times")
    println("PULocationID is null " + pq.where("PULocationID IS NULL").count.toString + " times")
    println("DOLocationID is null " + pq.where("DOLocationID IS NULL").count.toString + " times")
    println("payment_type is null " + pq.where("payment_type IS NULL").count.toString + " times")
    println("fare_amount is null " + pq.where("fare_amount IS NULL").count.toString + " times")
    println("extra is null " + pq.where("extra IS NULL").count.toString + " times")
    println("mta_tax is null " + pq.where("mta_tax IS NULL").count.toString + " times")
    println("tip_amount is null " + pq.where("tip_amount IS NULL").count.toString + " times")
    println("cbd_congestion_fee is null " + pq.where("cbd_congestion_fee IS NULL").count.toString + " times")
    println("tolls_amount is null " + pq.where("tolls_amount IS NULL").count.toString + " times")
    println("improvement_surcharge is null " + pq.where("improvement_surcharge IS NULL").count.toString + " times")
    println("total_amount is null " + pq.where("total_amount IS NULL").count.toString + " times")
    println("congestion_surcharge is null " + pq.where("congestion_surcharge IS NULL").count.toString + " times")
    println("Airport_fee is null "  + pq.where("Airport_fee IS NULL").count.toString + " times")
    println("Total rows in 2025-01: " + pq.count.toString)
    println("*********************************************")
    TripData.cleanup(pq).show()
    spark.stop()
  }
}
