package org.cscie88c.spark

import org.apache.spark.sql.{SparkSession, Dataset, functions => F}
import org.apache.spark.sql.Dataset

case class Movie(
  genre: String,
  category: String,
  title: String,
  year: Int,
  distribution: String,
  description: String,
  url: String,
  cover_photo: String
) {
  def isSuperhero = description.toLowerCase.indexOf("superhero")!= -1
  def isYetAnotherAvengersMovie = title.toLowerCase().contains("avengers")
}

case class MovieKPI(
  genre: String,
  total_titles: Long,
  active_years: Long,
  avg_title_length: Double,
  avg_description_length: Double,
  avengers_mentions: Long
)

// Spark job to run on GCP Dataproc
// Reads a CSV file with movie data, calculates aggregates, and saves output as Parquet
// Usage: spark-submit --class <main-class> <jar-file> <input-file-path> <output-path>
// e.g., /opt/spark/bin/spark-submit --class org.cscie88c.spark.SparkGCPJob /opt/spark-apps/SparkJob.jar /opt/spark-data/movies.csv /opt/spark-data/output/
// OR
// Create a batch job on GCP Dataproc with similar parameters. See https://cloud.google.com/dataproc-serverless/docs/overview

object SparkGCPJob {

  def main(args: Array[String]): Unit = {
    val Array(infile, outpath) = args
    implicit val spark = SparkSession.builder()
      .appName("GCPSparkJob")
      .master("local[*]")
      .getOrCreate()

    // 1. Load input file (bronze layer)
    val inputMovieData: Dataset[Movie] = loadInputFile(infile)

    // 2. Cleanup data (silver layer)
    val cleanMovieData: Dataset[Movie] = cleanData(inputMovieData)

    // 3. Calculate aggregate KPIs (gold layer)
    val movieKPI: Dataset[MovieKPI] = calculateKPIs(cleanMovieData)

    // 4. Save output
    saveOutput(movieKPI, outpath)

    spark.stop()
  }

  def loadInputFile(filePath: String)(implicit spark: SparkSession): Dataset[Movie] = {
    import spark.implicits._
    
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(filePath)
      .as[Movie]
  }

  def cleanData(inputData: org.apache.spark.sql.Dataset[Movie]): org.apache.spark.sql.Dataset[Movie] = {
    // Placeholder for data cleaning logic
    inputData.filter(F.col("title").isNotNull && F.col("genre").isNotNull)
  }

  def calculateKPIs(inputData: org.apache.spark.sql.Dataset[Movie])(implicit spark: SparkSession): org.apache.spark.sql.Dataset[MovieKPI] = {
    import spark.implicits._

    inputData
      .groupBy("genre")
      .agg(
        F.count("title").as("total_titles"),
        F.countDistinct("year").as("active_years"),
        F.avg(F.length(F.col("title"))).as("avg_title_length"),
        F.avg(F.length(F.col("description"))).as("avg_description_length"),
        F.sum(F.when(F.col("title").contains("Avengers"), 1).otherwise(0)).as("avengers_mentions")
      )
      .as[MovieKPI]
  }

  def saveOutput(kpis: org.apache.spark.sql.Dataset[MovieKPI], outputPath: String): Unit = {
    kpis.write
      .mode("overwrite")
      .parquet(outputPath)
  }
}
