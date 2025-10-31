package org.cscie88c.spark

import org.apache.spark.sql.SparkSession
import org.cscie88c.core.Utils

object SparkJob {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("SampleSparkJob")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._


    val df = TaxiZones.zonesFromFile("/opt/spark-data/taxi_zone_lookup.csv")(spark)
    val pq = TripData.loadParquetData("/opt/spark-data/yellow_tripdata_2025-01.parquet")(spark)

    df.show()
    pq.show(50)

    spark.stop()
  }
}
