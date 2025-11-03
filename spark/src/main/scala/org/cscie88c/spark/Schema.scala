package org.cscie88c.spark
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


// Following values are from the taxi_zone_lookup.csv

case class TaxiZoneSchema (
    LocationID: Int,
    Borough: String,
    Zone: String,
    Service_zone: String
    )
