package org.cscie88c.spark

import org.apache.spark.sql.{DataFrame, SparkSession}


case class TaxiZones (
    LocationID: Int,
    Borough: String,
    Zone: String,
    service_zone: String

                     )

object TaxiZones {
  def zonesFromFile(filePath: String) (implicit spark: SparkSession): DataFrame= {
    import spark.implicits._
    spark
      .read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(filePath)
  }
}