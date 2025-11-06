package org.cscie88c.spark

case class ComboYellowTripTaxiZoneSchema(
    // These are all headers in the trip parquet file
    // VendorID: String,
    tpep_pickup_datetime: String, // Required
    tpep_dropoff_datetime: String, // Required
    Pickup_Hour: Int,
    Pickup_Week: Int,
    Trip_Time: Double,
    // passenger_count: Int,
    trip_distance: Double, // Required
    // RatecodeID: String,
    // store_and_fwd_flag: Boolean,
    PULocationID: Int, // Required
    // Pickup info from TaxiZoneSchema table
    LocationIDPU: Int,
    BoroughPU: String,
    ZonePU: String,
    Service_zonePU: String,
    // These are all headers in the trip parquet file
    DOLocationID: Int, // Required
    // Dropoff info from TaxiZoneSchema table
    LocationIDDO: Int,
    BoroughDO: String,
    ZoneDO: String,
    Service_zoneDO: String,
    // These are all headers in the trip parquet file
    // payment_type: Int,
    fare_amount: Double // Required
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
