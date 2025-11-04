package org.cscie88c.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset

case class TripData (
    VendorID: Int,
    tpep_pickup_datetime: java.sql.Timestamp,
    tpep_dropoff_datetime: java.sql.Timestamp,
    passenger_count: Long,
    trip_distance: Double,
    RatecodeID: Long,
    store_and_fwd_flag: String,
    PULocationID: Int,
    DOLocationID: Int,
    payment_type: Long,
    fare_amount: Double,
    extra: Double,
    mta_tax: Double,
    tip_amount: Double,
    tolls_amount: Double,
    improvement_surcharge: Double,
    total_amount: Double,
    congestion_surcharge: Double,
    Airport_fee: Double,
    cbd_congestion_fee: Double
)
object TripData {
  def loadParquetData(filePath: String) (implicit spark: SparkSession): Dataset[TripData] = {
    import spark.implicits._
    spark
      .read
      .option("mergeSchema", "true")
      .parquet(filePath)
      .as[TripData]
      

  }

  def cleanup(ds: Dataset[TripData]): Dataset[TripData] = {
    ds.where("passenger_count < 0").union(
      ds.where("passenger_count > 14")).union(
      ds.where("PULocationID > 265")).union(
      ds.where("PULocationID < 1")).union(
      ds.where("DOLocationID > 265")).union(
      ds.where("DOLocationID < 1")).union(
      ds.where("payment_type > 5")).union(
      ds.where("payment_type < 0")).union(
      ds.where("total_amount > 300"))

  }
}
