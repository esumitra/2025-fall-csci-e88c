package org.cscie88c

import org.apache.spark.sql.{SparkSession, Dataset}
import org.cscie88c.spark.{YellowTripSchema, TaxiZoneSchema}
import org.apache.spark.sql.functions._

object BronzeDataIngestion {

    def loadParquetFile(filePath: String)(implicit spark: SparkSession): Dataset[YellowTripSchema] = {
        import spark.implicits._

        // Get field names from schema with a sequence
        val columns: Seq[String] = Seq(
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "trip_distance",
        "PULocationID",
        "DOLocationID",
        "fare_amount"
        )

        spark.read
        .format("parquet")
        .option("header", "true")
        // .option("inferSchema", "true") // Not sure if needed
        .parquet(filePath)
        .select(columns.map(col): _*) // Selects only needed columns defined above
        // Can alternatively be used to avoid mapping above
        // .select("tpep_pickup_datetime",
        //         "tpep_dropoff_datetime",
        //         "trip_distance",
        //         "PULocationID",
        //         "DOLocationID",
        //         "fare_amount"
        //         )
        .as[YellowTripSchema]
   }

   def loadCSVFile(filePath: String)(implicit spark: SparkSession): Dataset[TaxiZoneSchema] = {
        import spark.implicits._
    
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(filePath)
        .as[TaxiZoneSchema]
   }
}
