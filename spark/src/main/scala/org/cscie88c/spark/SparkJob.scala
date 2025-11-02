package org.cscie88c.spark

import org.apache.spark.sql.SparkSession
import org.cscie88c.core.Utils

object SparkJob {
  // def main(args: Array[String]): Unit = {
  //   val spark = SparkSession.builder()
  //     .appName("SampleSparkJob")
  //     .master("local[*]")
  //     .getOrCreate()

  //   import spark.implicits._

  //   val df = Seq("Alice", "Bob", "Carol").toDF("name")
  //   val greeted = df.map(row => Utils.greet(row.getString(0)))

  //   greeted.show(false)
  //   spark.stop()
  // }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("BronzeDataReader")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val filePath = "../Data/bronze/yellow_tripdata_2025-01.parquet"
    val filePath2 = "../Data/bronze/taxi_zone_lookup.csv"

    val YellowTrip_df = spark.read.parquet(filePath)
    val TaxiZone_df = spark.read.csv(filePath2)

    // Test Section
    println(s"Schema for $filePath:")
    YellowTrip_df.show(5, truncate = false)  // Sample a few rows to inspect

    println(s"Schema for $filePath2:")
    TaxiZone_df.show(5, truncate = false)  // Sample a few rows to inspect

    spark.stop()
  }
}
