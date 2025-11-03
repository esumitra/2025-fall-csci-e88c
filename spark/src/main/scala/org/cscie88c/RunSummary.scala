package org.cscie88c

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.cscie88c.spark.{YellowTripSchema, TaxiZoneSchema}
import org.apache.spark.sql.functions._

object RunSummary {

  def generateSummary(
      trips: Dataset[YellowTripSchema],
      zones: Dataset[TaxiZoneSchema]
  )(implicit spark: SparkSession): DataFrame = {

      import spark.implicits._

      // Row count
      val recordCount = trips.count()

      // Nulls
      val nullsDF = DataQualityChecks.nullPercentages(trips.toDF)
          .withColumnRenamed("null_percent", "value")
          .withColumn("metric", concat(lit("null_percent_"), col("column")))
          .select("metric", "value")

      // Range checks
      val rangeDF = DataQualityChecks.rangeChecks(trips)
          .selectExpr("stack(3, 'invalid_trip_distance', invalid_trip_distance, 'invalid_fare_amount', invalid_fare_amount, 'invalid_time_order', invalid_time_order) as (metric, value)")

      // Referential checks
      val refDF = DataQualityChecks.referentialCheck(trips, zones)
          .withColumnRenamed("check_type", "metric")
          .withColumnRenamed("invalid_count", "value")

      // Combine together
      val totalDF = nullsDF.union(rangeDF).union(refDF)
          .union(Seq(("record_count", recordCount.toDouble)).toDF("metric", "value"))

      totalDF
  }
}
