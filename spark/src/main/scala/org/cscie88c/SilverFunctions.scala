package org.cscie88c

import org.apache.spark.sql.{DataFrame}
import org.apache.spark.sql.functions._

object SilverFunctions {

// Gather week data with simple function
def getWeekData(df: DataFrame, weekNum: Int): DataFrame =
  df.filter(col("Pickup_Week") === weekNum)


// Silver functions to remove invalid records

// Trip distance must be greater than 0, with no upper limit
def validTripDistance(df: DataFrame): DataFrame = {
  df.filter(col("trip_distance") > 0)
}

// Fare amount must be greater than 0, with no upper limit
def validFareAmount(df: DataFrame): DataFrame = {
  df.filter(col("fare_amount") > 0)
}

// Customer must be picked up before being dropped off
def validTimeOrder(df: DataFrame): DataFrame = {
  df.filter(col("tpep_dropoff_datetime") > col("tpep_pickup_datetime"))
}
}
