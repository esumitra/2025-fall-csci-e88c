package org.cscie88c

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.cscie88c.spark.{YellowTripSchema, TaxiZoneSchema}
import org.apache.spark.sql.functions._

object DataQualityChecks {

    // Null percentage check
    def nullPercentages(df: DataFrame)(implicit spark: SparkSession): DataFrame = {

        import spark.implicits._

        val total = df.count().toDouble                 // Total number of rows in the DataFrame

        val nullStats = df.columns.toSeq.map {          // Iterate over all column names
            colName => val nullCount = 
                df.filter(df(colName).isNull).count()   // Count how many rows have nulls in this column
            (colName, nullCount / total * 100)          // Compute percentage of nulls per column
        }

        nullStats.toDF("column", "null_percent")        // Convert results back into DataFrame
    }

    // Range checks
    def rangeChecks(trips: Dataset[YellowTripSchema]): DataFrame = {

        import trips.sparkSession.implicits._

        trips
            .withColumn("valid_trip_distance", col("trip_distance") > 0)    // Boolean column indicating whether trip distance > 0
            .withColumn("valid_fare_amount", col("fare_amount") > 0)        // Boolean column indicating whether fare amount > 0
            .withColumn("pickup_before_dropoff",                            // boolean column indicating whether pickup time is before dropoff time
            unix_timestamp(col("tpep_pickup_datetime")) < unix_timestamp(col("tpep_dropoff_datetime"))
            )
            .agg(       // Aggregate invalid counts by summing rows that fail each condition
            sum(when(!col("valid_trip_distance"), 1).otherwise(0)).alias("invalid_trip_distance"),
            sum(when(!col("valid_fare_amount"), 1).otherwise(0)).alias("invalid_fare_amount"),
            sum(when(!col("pickup_before_dropoff"), 1).otherwise(0)).alias("invalid_time_order")
            )
    }

    // Referential integrity
    def referentialCheck(
        trips: Dataset[YellowTripSchema],
        zones: Dataset[TaxiZoneSchema]
        )(implicit spark: SparkSession): DataFrame = {

        import spark.implicits._

        val zoneIDs = zones.select(col("LocationID").alias("ZoneID"))

        val invalidPU = trips.join(zoneIDs, trips("PULocationID") === zoneIDs("ZoneID"), "left_anti").count()
        val invalidDO = trips.join(zoneIDs, trips("DOLocationID") === zoneIDs("ZoneID"), "left_anti").count()

        Seq(("invalid_PULocationIDs", invalidPU), ("invalid_DOLocationIDs", invalidDO))
            .toDF("check_type", "invalid_count")
        }
}