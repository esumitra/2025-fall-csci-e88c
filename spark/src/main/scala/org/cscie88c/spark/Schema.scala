package org.cscie88c.spark
import org.apache.spark.sql.types._

case class TripSchema(

    // These are all headers in the trip parquet file
    VendorID: String,
    tpep_pickup_datetime: String,
    tpep_dropoff_datetime: String,
    passenger_count: Int,
    trip_distance: Double,
    RatecodeID: String,
    store_and_fwd_flag: Boolean,
    PULocationID: String,
    DOLocationID: String,
    payment_type: Int,
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


// Following values are from the taxi_zone_lookup.csv

case class BoroughSchema (
    LocationID: Int,
    Borough: String,
    Zone: Int,              // Likely needs adjusting
    Service_zone: String    // Likely needs adjusting
    )
