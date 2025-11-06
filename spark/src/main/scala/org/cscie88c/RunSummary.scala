package org.cscie88c

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object RunSummary {

  def generateSummary(df: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    // Row count
    val recordCount = df.count()

    // Null percentage summary
    val nullsDF = DataQualityChecks.nullPercentages(df)
      .withColumnRenamed("null_percent", "value")
      .withColumn("metric", concat(lit("null_percent_"), col("column")))
      .select("metric", "value")

    // Range checks summary
    val rangeDF = DataQualityChecks.rangeChecks(df)
      .select("metric", "value")

    // Combine everything together
    val totalDF = nullsDF
      .union(rangeDF)
      .union(Seq(("record_count", recordCount.toDouble)).toDF("metric", "value"))

    totalDF
  }
}
