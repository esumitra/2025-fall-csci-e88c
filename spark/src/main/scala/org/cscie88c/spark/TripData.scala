package org.cscie88c.spark

import org.apache.spark.sql.SparkSession

case class TripData (
    VendorID: Int,
    tpep_pickup_datetime: Long,
    tpep_dropoff_datetime: Long,
    passenger_count: Long,
    trip_distance: Double,
    RatecodeID: Long,
    store_and_fwd_flag: String,
    PULocationID: Long,
    DOLocationID: Long,
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
  def loadParquetData(filePath: String) (implicit spark: SparkSession): org.apache.spark.sql.Dataset[TripData] = {
    import spark.implicits._
    spark
      .read
      .parquet(filePath)
      .as[TripData]

  }
}
