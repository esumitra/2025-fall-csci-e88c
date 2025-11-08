package org.cscie88c.spark

import org.apache.spark.sql.SparkSession
import org.cscie88c.core.Utils

object BronzeJob {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("SampleSparkJob")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._


    val df = TaxiZones.zonesFromFile("/opt/spark-data/taxi_zone_lookup.csv")(spark)
    val pq = TripData.loadParquetData("/opt/spark-data/yellow_tripdata_2025-01.parquet")(spark)
    println("Zone sample")
    df.show(10)
    println("Null percentatages by column:" )
    calculateNullPercentages(pq)(spark).show(100, false)
    spark.stop()
  }


  def calculateNullPercentages(df: DataFrame)(spark: SparkSession): DataFrame = {
    // Calculate counts for all columns in a single pass
    val exprs = df.columns.map { colName =>
      (count(when(col(colName).isNull, true)) / count("*") * 100).alias(s"${colName}_null_pct")
    }

    // Run the aggregation
    val nullCounts = df.select(exprs: _*).first()

    // Transform row to column format
    val result = df.columns.map { colName =>
      (colName, nullCounts.getAs[Double](s"${colName}_null_pct"))
    }

    // Convert to DataFrame
    spark.createDataFrame(result)
      .toDF("column_name", "null_percentage")
      .orderBy(desc("null_percentage"))
  }

}
