package org.cscie88c

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object DataQualityChecks {

    // Null percentage check
    def nullPercentages(df: DataFrame)(implicit spark: SparkSession): DataFrame = {

        import spark.implicits._

        val total = df.count().toDouble

        // Compute ratio of nulls per column
        val nullStats = df.columns.toSeq.map { colName =>
            val nullCount = df.filter(col(colName).isNull || col(colName) === "").count()
            (colName, nullCount / total * 100)
        }

        nullStats.toDF("column", "null_percent")
    }

    // Range checks
    def invalidTripDistance(df: DataFrame): DataFrame = {
        df.filter(col("trip_distance") <= 0)
    }

    def invalidFareAmount(df: DataFrame): DataFrame = {
        df.filter(col("fare_amount") <= 0)
    }

    def invalidTimeOrder(df: DataFrame): DataFrame = {
        df.filter(
        unix_timestamp(col("tpep_dropoff_datetime")) <= unix_timestamp(col("tpep_pickup_datetime"))
        )
    }

    // Used to help generate summary of invalid 
    def rangeChecks(df: DataFrame): DataFrame = {
        import df.sparkSession.implicits._

        val invalidTripDistanceCount = invalidTripDistance(df).count()
        val invalidFareAmountCount = invalidFareAmount(df).count()
        val invalidTimeOrderCount = invalidTimeOrder(df).count()

        Seq(
        ("invalid_trip_distance", invalidTripDistanceCount),
        ("invalid_fare_amount", invalidFareAmountCount),
        ("invalid_time_order", invalidTimeOrderCount)
        ).toDF("metric", "value")
    }

    // Referential integrity (NEEDS REVIEW)
    def referentialCheck(df: DataFrame, zones: DataFrame)(implicit spark: SparkSession): DataFrame = ???
}