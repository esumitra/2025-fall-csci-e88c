package org.cscie88c

import org.apache.spark.sql.{DataFrame, SparkSession, Dataset}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{from_unixtime, hour, to_date, datediff}

object SilverFunctions {

// ability to handle DFs here was not working at all,
// using SparkJob.scala -- sorry

def getWeekData(df: DataFrame, weekNum: Int): DataFrame =
  df.filter(col("Pickup_Week") === weekNum)

def isValidTripDistance(df: DataFrame): Boolean = ???

def isValidFareAmount(df: DataFrame): Boolean = ???

def isValidTimeOrder(df: DataFrame): Boolean = ???
}
