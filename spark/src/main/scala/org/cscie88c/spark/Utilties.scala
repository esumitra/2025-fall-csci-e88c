package org.cscie88c.spark

import org.apache.spark.sql.{Column, DataFrame}

object Utilities {
  def parquetOutput(dataFrame: DataFrame, fileName: String): Unit = {
    dataFrame
      .coalesce(1)
      .write
      .mode("overwrite")
      .option("compression", "snappy")
      .parquet(fileName)
  }
  def csvOutput(
      dataFrame: DataFrame,
      fileName: String,
      rowLimit: Int,
      sortExpr: Column
  ): Unit = {
    dataFrame
      .orderBy(sortExpr)
      .limit(rowLimit)
      .coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .option("quoteAll", "true")
      .csv(fileName)
  }
  val SilverTripsConformed = "/opt/spark-data/silver/trips_conformed"
}
