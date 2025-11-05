package org.cscie88c.spark

import org.cscie88c.spark.{YellowTripSchema, TaxiZoneSchema}
import org.cscie88c.{RunSummary, BronzeDataIngestion, DataQualityChecks, SilverFunctions}
import org.apache.spark.sql.{DataFrame, SparkSession, Dataset}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{from_unixtime, hour, to_date, datediff}

object SparkJob {
  // Original scaffolding code

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
      val Array(infile, outpath) = args
      implicit val spark = SparkSession.builder()
              .appName("BronzeDataIngestion")
              .master("local[*]")
              .getOrCreate()
      
      import spark.implicits._

      val yellowTripDataFilePath = "../data/bronze/yellow_tripdata_2025-01.parquet"
      val taxiZoneLookupFilePath = "../data/bronze/taxi_zone_lookup.csv"

      val tripsDS: Dataset[YellowTripSchema] =  BronzeDataIngestion.loadParquetFile(yellowTripDataFilePath)
      val zonesDS: Dataset[TaxiZoneSchema] = BronzeDataIngestion.loadCSVFile(taxiZoneLookupFilePath)
      
      // joined/linked table will need DataFrames, then create based off PU or DO
      val tripsDF = tripsDS.toDF().alias("trips")
      val zonesDF = zonesDS.toDF().alias("zones")      
      val joinedTripsZonesDFPU = tripsDF.join(
        zonesDF,
        tripsDF("PULocationID") === zonesDF("LocationID"),
        "inner"
      )
      val joinedTripsZonesDFDO = tripsDF.join(
        zonesDF,
        tripsDF("DOLocationID") === zonesDF("LocationID"),
        "inner"
      )

      // joined table from PU and DO zone information incorporated
      val joinedTripsZonesDFRequirements = (
        ((joinedTripsZonesDFDO("tpep_pickup_datetime")) === (joinedTripsZonesDFPU("tpep_pickup_datetime"))) &&
        ((joinedTripsZonesDFDO("tpep_dropoff_datetime")) === (joinedTripsZonesDFPU("tpep_dropoff_datetime"))) &&
        ((joinedTripsZonesDFDO("trip_distance")) === (joinedTripsZonesDFPU("trip_distance"))) &&
        ((joinedTripsZonesDFDO("PULocationID")) === (joinedTripsZonesDFPU("PULocationID"))) &&
        ((joinedTripsZonesDFDO("DOLocationID")) === (joinedTripsZonesDFPU("DOLocationID"))) &&
        ((joinedTripsZonesDFDO("fare_amount")) === (joinedTripsZonesDFPU("fare_amount")))
      )
      val joinedTripsZonesDF = joinedTripsZonesDFDO.alias("DO").join(
        joinedTripsZonesDFPU.alias("PU"),
        joinedTripsZonesDFRequirements,
        "inner"
      )

      // create a table without duplicate rows from the joined table
      val ComboYellowTripTaxiZonesDF = joinedTripsZonesDF.select(
        col("PU.tpep_pickup_datetime").as("tpep_pickup_datetime"),
        col("PU.tpep_dropoff_datetime").as("tpep_dropoff_datetime"),
        col("PU.trip_distance").as("distance"),
        col("PU.PULocationID").as("PULocation"),
        col("PU.LocationID").as("LocationIDPU"),
        col("PU.Borough").as("BoroughPU"),
        col("PU.Zone").as("ZonePU"),
        col("PU.Service_zone").as("Service_zonePU"),
        col("DO.DOLocationID").as("DOLocation"),
        col("DO.LocationID").as("LocationIDDO"),
        col("DO.Borough").as("BoroughDO"),
        col("DO.Zone").as("ZoneDO"),
        col("DO.Service_zone").as("Service_zoneDO"),
        col("PU.fare_amount").as("fare_amount")
      )

      val combinedDF = ComboYellowTripTaxiZonesDF
        .withColumn("Pickup_Hour", hour(col("tpep_pickup_datetime")))
        .withColumn("Pickup_Week", weekofyear(col("tpep_pickup_datetime")))
        .withColumn("Trip_Time", unix_timestamp(to_timestamp($"tpep_dropoff_datetime")) - unix_timestamp(to_timestamp($"tpep_pickup_datetime")))
        .withColumn("Trip_Time_Min_Converted", col("Trip_Time").cast("double") / 60) // Converted from sec to min 

        // Chi's Code from Announcement
        // new columns for Part B requirements
        // Peak-Hour Trip %
        .withColumn("Is_Peak_Hour", when(col("Pickup_Hour").between(7,9) // Morning Peak (7-9) = 1
          .or(col("Pickup_Hour").between(16,18)), 1).otherwise(0)) // Evening Peak (16-18) = 1 otherwise 0
        // Weekly Trip Volume by Borough
        .withColumn("Week_Borough", concat_ws("_", col("Pickup_Week"), col("BoroughPU"))) // get week and borough e.g., "1_Manhattan"  
        // Avg Trip Time vs Distance
        .withColumn("Avg_Trip_Speed_Mph", when(col("Trip_Time_Min_Converted") > 0, 
          (col("distance") / col("Trip_Time_Min_Converted")) * 60).otherwise(0)) // distance / time (in hours) in mph
        // Total Trips & Total Revenue (Weekly)
        .withColumn("Week_Borough_Revenue", concat_ws("_", col("Pickup_Week"), col("BoroughPU"))) // get week and borough e.g., "1_Manhattan"
        // Avg Revenue per Mile
        .withColumn("Revenue_per_Mile", when(col("distance") > 0,
          col("fare_amount") / col("distance")).otherwise(0)) // fare amount / distance
        // Night Trip %
        .withColumn("Is_Night_Trip", when(col("Pickup_Hour").between(21,23) // Nighttime (9PM-11PM) = 1
          .or(col("Pickup_Hour").between(0,3)), 1).otherwise(0)) // Early Morning (12AM-3AM) = 1 otherwise 0


      // Review Section
      spark.sparkContext.setLogLevel("ERROR") // Show reduce log bloat for testing
      

      println("--- CombinedDF Review ---")
      combinedDF.show(5, truncate = false)
      println("--- End CombinedDF Review ---")

      /*
      [info] +--------------------+---------------------+--------+----------+------------+---------+-----------------------+--------------+----------+------------+---------+------------------------+--------------+-----------+-----------+-----------+---------+-----------------------+------------+------------+------------------+--------------------+------------------+-------------+
      [info] |tpep_pickup_datetime|tpep_dropoff_datetime|distance|PULocation|LocationIDPU|BoroughPU|ZonePU                 |Service_zonePU|DOLocation|LocationIDDO|BoroughDO|ZoneDO                  |Service_zoneDO|fare_amount|Pickup_Hour|Pickup_Week|Trip_Time|Trip_Time_Min_Converted|Is_Peak_Hour|Week_Borough|Avg_Trip_Speed_Mph|Week_Borough_Revenue|Revenue_per_Mile  |Is_Night_Trip|
      [info] +--------------------+---------------------+--------+----------+------------+---------+-----------------------+--------------+----------+------------+---------+------------------------+--------------+-----------+-----------+-----------+---------+-----------------------+------------+------------+------------------+--------------------+------------------+-------------+
      [info] |2024-12-31 21:33:43 |2024-12-31 21:39:00  |1.12    |179       |179         |Queens   |Old Astoria            |Boro Zone     |7         |7           |Queens   |Astoria                 |Boro Zone     |7.9        |21         |1          |317      |5.283333333333333      |0           |1_Queens    |12.719242902208205|1_Queens            |7.053571428571428 |1            |
      [info] |2025-01-01 00:00:07 |2025-01-01 00:06:31  |0.93    |148       |148         |Manhattan|Lower East Side        |Yellow Zone   |45        |45          |Manhattan|Chinatown               |Yellow Zone   |-7.9       |0          |1          |384      |6.4                    |0           |1_Manhattan |8.71875           |1_Manhattan         |-8.494623655913978|1            |
      [info] |2025-01-01 00:00:17 |2025-01-01 00:15:22  |5.6     |132       |132         |Queens   |JFK Airport            |Airports      |10        |10          |Queens   |Baisley Park            |Boro Zone     |22.6       |0          |1          |905      |15.083333333333334     |0           |1_Queens    |22.27624309392265 |1_Queens            |4.0357142857142865|1            |
      [info] |2025-01-01 00:01:00 |2025-01-01 00:07:39  |3.63    |238       |238         |Manhattan|Upper West Side North  |Yellow Zone   |244       |244         |Manhattan|Washington Heights South|Boro Zone     |15.6       |0          |1          |399      |6.65                   |0           |1_Manhattan |32.751879699248114|1_Manhattan         |4.297520661157025 |1            |
      [info] |2025-01-01 00:02:42 |2025-01-01 00:14:13  |2.2     |113       |113         |Manhattan|Greenwich Village North|Yellow Zone   |233       |233         |Manhattan|UN/Turtle Bay South     |Yellow Zone   |12.1       |0          |1          |691      |11.516666666666667     |0           |1_Manhattan |11.461649782923299|1_Manhattan         |5.499999999999999 |1            |
      [info] +--------------------+---------------------+--------+----------+------------+---------+-----------------------+--------------+----------+------------+---------+------------------------+--------------+-----------+-----------+-----------+---------+-----------------------+------------+------------+------------------+--------------------+------------------+-------------+
      */

      // ** Definitely a way to loop this, but its small enough to block create **
      val week1 = SilverFunctions.getWeekData(combinedDF, 1)
      val week2 = SilverFunctions.getWeekData(combinedDF, 2)
      val week3 = SilverFunctions.getWeekData(combinedDF, 3)
      val week4 = SilverFunctions.getWeekData(combinedDF, 4)
      val week5 = SilverFunctions.getWeekData(combinedDF, 5)

      // Block Review
      println("--- Weeks 1-5 Review ---")
      week1.show(2, truncate = false)
      week2.show(2, truncate = false)
      week3.show(2, truncate = false)
      week4.show(2, truncate = false)
      week5.show(2, truncate = false)
      println("--- End Weeks 1-5 Review ---")

      /*
      +--------------------+---------------------+--------+----------+------------+---------+---------------+--------------+----------+------------+---------+---------+--------------+-----------+-----------+-----------+---------+-----------------------+
      [info] |tpep_pickup_datetime|tpep_dropoff_datetime|distance|PULocation|LocationIDPU|BoroughPU|ZonePU         |Service_zonePU|DOLocation|LocationIDDO|BoroughDO|ZoneDO   |Service_zoneDO|fare_amount|Pickup_Hour|Pickup_Week|Trip_Time|Trip_Time_Min_Converted|
      [info] +--------------------+---------------------+--------+----------+------------+---------+---------------+--------------+----------+------------+---------+---------+--------------+-----------+-----------+-----------+---------+-----------------------+
      [info] |2024-12-31 21:33:43 |2024-12-31 21:39:00  |1.12    |179       |179         |Queens   |Old Astoria    |Boro Zone     |7         |7           |Queens   |Astoria  |Boro Zone     |7.9        |21         |1          |317      |5.283333333333333      |
      [info] |2025-01-01 00:00:07 |2025-01-01 00:06:31  |0.93    |148       |148         |Manhattan|Lower East Side|Yellow Zone   |45        |45          |Manhattan|Chinatown|Yellow Zone   |-7.9       |0          |1          |384      |6.4                    |
      [info] +--------------------+---------------------+--------+----------+------------+---------+---------------+--------------+----------+------------+---------+---------+--------------+-----------+-----------+-----------+---------+-----------------------+
      [info] only showing top 2 rows
      [info] +--------------------+---------------------+--------+----------+------------+---------+------------+--------------+----------+------------+---------+-------------------------+--------------+-----------+-----------+-----------+---------+-----------------------+
      [info] |tpep_pickup_datetime|tpep_dropoff_datetime|distance|PULocation|LocationIDPU|BoroughPU|ZonePU      |Service_zonePU|DOLocation|LocationIDDO|BoroughDO|ZoneDO                   |Service_zoneDO|fare_amount|Pickup_Hour|Pickup_Week|Trip_Time|Trip_Time_Min_Converted|
      [info] +--------------------+---------------------+--------+----------+------------+---------+------------+--------------+----------+------------+---------+-------------------------+--------------+-----------+-----------+-----------+---------+-----------------------+
      [info] |2025-01-06 00:00:23 |2025-01-06 00:39:52  |20.06   |132       |132         |Queens   |JFK Airport |Airports      |230       |230         |Manhattan|Times Sq/Theatre District|Yellow Zone   |70.0       |0          |2          |2369     |39.483333333333334     |
      [info] |2025-01-06 00:00:36 |2025-01-06 00:03:32  |0.73    |50        |50          |Manhattan|Clinton West|Yellow Zone   |230       |230         |Manhattan|Times Sq/Theatre District|Yellow Zone   |5.8        |0          |2          |176      |2.933333333333333      |
      [info] +--------------------+---------------------+--------+----------+------------+---------+------------+--------------+----------+------------+---------+-------------------------+--------------+-----------+-----------+-----------+---------+-----------------------+
      [info] only showing top 2 rows
      [info] +--------------------+---------------------+--------+----------+------------+---------+----------------------------+--------------+----------+------------+---------+--------------+--------------+-----------+-----------+-----------+---------+-----------------------+
      [info] |tpep_pickup_datetime|tpep_dropoff_datetime|distance|PULocation|LocationIDPU|BoroughPU|ZonePU                      |Service_zonePU|DOLocation|LocationIDDO|BoroughDO|ZoneDO        |Service_zoneDO|fare_amount|Pickup_Hour|Pickup_Week|Trip_Time|Trip_Time_Min_Converted|
      [info] +--------------------+---------------------+--------+----------+------------+---------+----------------------------+--------------+----------+------------+---------+--------------+--------------+-----------+-----------+-----------+---------+-----------------------+
      [info] |2025-01-13 00:00:16 |2025-01-13 00:15:48  |3.91    |186       |186         |Manhattan|Penn Station/Madison Sq West|Yellow Zone   |263       |263         |Manhattan|Yorkville West|Yellow Zone   |21.16      |0          |3          |932      |15.533333333333333     |
      [info] |2025-01-13 00:00:43 |2025-01-13 00:06:39  |1.38    |186       |186         |Manhattan|Penn Station/Madison Sq West|Yellow Zone   |249       |249         |Manhattan|West Village  |Yellow Zone   |8.6        |0          |3          |356      |5.933333333333334      |
      [info] +--------------------+---------------------+--------+----------+------------+---------+----------------------------+--------------+----------+------------+---------+--------------+--------------+-----------+-----------+-----------+---------+-----------------------+
      [info] only showing top 2 rows
      [info] +--------------------+---------------------+--------+----------+------------+---------+-------------------------+--------------+----------+------------+---------+--------------------+--------------+-----------+-----------+-----------+---------+-----------------------+
      [info] |tpep_pickup_datetime|tpep_dropoff_datetime|distance|PULocation|LocationIDPU|BoroughPU|ZonePU                   |Service_zonePU|DOLocation|LocationIDDO|BoroughDO|ZoneDO              |Service_zoneDO|fare_amount|Pickup_Hour|Pickup_Week|Trip_Time|Trip_Time_Min_Converted|
      [info] +--------------------+---------------------+--------+----------+------------+---------+-------------------------+--------------+----------+------------+---------+--------------------+--------------+-----------+-----------+-----------+---------+-----------------------+
      [info] |2025-01-20 00:00:05 |2025-01-20 00:07:42  |3.42    |230       |230         |Manhattan|Times Sq/Theatre District|Yellow Zone   |151       |151         |Manhattan|Manhattan Valley    |Yellow Zone   |15.6       |0          |4          |457      |7.616666666666666      |
      [info] |2025-01-20 00:00:28 |2025-01-20 00:29:44  |11.49   |33        |33          |Brooklyn |Brooklyn Heights         |Boro Zone     |42        |42          |Manhattan|Central Harlem North|Boro Zone     |43.79      |0          |4          |1756     |29.266666666666666     |
      [info] +--------------------+---------------------+--------+----------+------------+---------+-------------------------+--------------+----------+------------+---------+--------------------+--------------+-----------+-----------+-----------+---------+-----------------------+
      [info] only showing top 2 rows
      [info] +--------------------+---------------------+--------+----------+------------+---------+-------------------+--------------+----------+------------+---------+-------------------+--------------+-----------+-----------+-----------+---------+-----------------------+
      [info] |tpep_pickup_datetime|tpep_dropoff_datetime|distance|PULocation|LocationIDPU|BoroughPU|ZonePU             |Service_zonePU|DOLocation|LocationIDDO|BoroughDO|ZoneDO             |Service_zoneDO|fare_amount|Pickup_Hour|Pickup_Week|Trip_Time|Trip_Time_Min_Converted|
      [info] +--------------------+---------------------+--------+----------+------------+---------+-------------------+--------------+----------+------------+---------+-------------------+--------------+-----------+-----------+-----------+---------+-----------------------+
      [info] |2025-01-27 00:00:32 |2025-01-27 00:12:54  |2.68    |142       |142         |Manhattan|Lincoln Square East|Yellow Zone   |234       |234         |Manhattan|Union Sq           |Yellow Zone   |14.2       |0          |5          |742      |12.366666666666667     |
      [info] |2025-01-27 00:00:35 |2025-01-27 00:12:52  |3.03    |48        |48          |Manhattan|Clinton East       |Yellow Zone   |144       |144         |Manhattan|Little Italy/NoLiTa|Yellow Zone   |14.9       |0          |5          |737      |12.283333333333333     |
      [info] +--------------------+---------------------+--------+----------+------------+---------+-------------------+--------------+----------+------------+---------+-------------------+--------------+-----------+-----------+-----------+---------+-----------------------+
      */

      // ** I made a helper function for this, so I'll comment this out to review **
      // val week1Data = combinedDF.filter($"Pickup_Week" === "1")
      // val week2Data = combinedDF.filter($"Pickup_Week" === "2")
      // val week3Data = combinedDF.filter($"Pickup_Week" === "3")
      // val week4Data = combinedDF.filter($"Pickup_Week" === "4")
      // val week5Data = combinedDF.filter($"Pickup_Week" === "5")
      //week2Data.show(10, truncate = false)

      // I made changes to this and got output below -Greg
      val boroughVol = combinedDF
        .join(
          zonesDF, 
          combinedDF("LocationIDPU") === zonesDF("LocationID"),
          "left")
        .groupBy("Borough", "Pickup_Week")
        .count()
        .withColumnRenamed("count", "TripVolume")
        .orderBy("Borough", "Pickup_Week")

      // For testing uncomment this
      println("--- Borough Volume Review ---")
      boroughVol.show(5, truncate = false)
      println("--- End Borough Volume Review ---")

      /*
      [info] +-------+-----------+----------+
      [info] |Borough|Pickup_Week|TripVolume|
      [info] +-------+-----------+----------+
      [info] |Bronx  |1          |1799      |
      [info] |Bronx  |2          |2713      |
      [info] |Bronx  |3          |3972      |
      [info] |Bronx  |4          |3587      |
      [info] |Bronx  |5          |2670      |
      [info] +-------+-----------+----------+
      */


      // ** I'm going to try to recreate this below **
      // avg trip (exact route) time per distance -- need to join Zone info x2
      // val tripTimeperDistance = combinedDF
      //   .withColumn("TimeperDistance", col("Trip_Time") / col("distance"))
      //   .groupBy("PULocation", "DOLocation")
      //   .avg("TimeperDistance")
      //   .withColumnRenamed("avg(TimeperDistance)", "AvgTimeperDistance").join(
      //     zonesDF,
      //     combinedDF("")
      //   )
      // tripTimeperDistance.show(10, truncate = false)

      val tripTimePerDistance = combinedDF
        .withColumn("TimePerDistance", col("Trip_Time_Min_Converted") / col("distance"))
        .filter(col("distance") > 0) // Avoid divide by zero error
        .join(
          zonesDF.select(
            col("LocationID").alias("PULocation"),
            col("Zone").alias("PUZone"),
            col("Borough").alias("PUBorough")
          ),
          Seq("PULocation"),
          "left"
        )
        .join(
          zonesDF.select(
            col("LocationID").alias("DOLocation"),
            col("Zone").alias("DOZone"),
            col("Borough").alias("DOBorough")
          ),
          Seq("DOLocation"),
          "left"
        )
        .groupBy("PUZone", "DOZone", "Pickup_Week")
        .agg(avg("TimePerDistance").alias("AvgTimePerDistance"))
        .orderBy(desc("AvgTimePerDistance"))

        println("--- Trip Time Per Distance Review ---")
        tripTimePerDistance.show(10, truncate = false)
        println("--- End Trip Time Per Distance Review ---")

        /*
        [info] +------------------------------+-------------------+-----------+------------------+
        [info] |PUZone                        |DOZone             |Pickup_Week|AvgTimePerDistance|
        [info] +------------------------------+-------------------+-----------+------------------+
        [info] |Garment District              |Far Rockaway       |4          |6453.333333333333 |
        [info] |Kensington                    |East Harlem South  |3          |5540.0            |
        [info] |Flushing                      |Crown Heights North|3          |5385.0            |
        [info] |Queensbridge/Ravenswood       |Ozone Park         |5          |5081.666666666667 |
        [info] |Park Slope                    |JFK Airport        |5          |5063.333333333333 |
        [info] |Williamsburg (South Side)     |Fresh Meadows      |5          |4921.666666666667 |
        [info] |Ocean Hill                    |Midwood            |5          |4383.333333333333 |
        [info] |Roosevelt Island              |Bath Beach         |3          |4375.0            |
        [info] |Mott Haven/Port Morris        |Jamaica            |5          |4326.666666666666 |
        [info] |Long Island City/Hunters Point|Clinton Hill       |2          |4211.666666666667 |
        [info] +------------------------------+-------------------+-----------+------------------+
        */

      // Test Section
      spark.read.parquet(yellowTripDataFilePath).printSchema()

      println(s"How many records in the parquet file: ${tripsDS.count()}")

      println(s"Schema for $yellowTripDataFilePath:")
      tripsDS.show(5, truncate = false)  // Sample a few rows to inspect

      println(s"Schema for $taxiZoneLookupFilePath:")
      zonesDS.show(5, truncate = false)  // Sample a few rows to inspect

      println(s"Schema for JOINED table:")
      ComboYellowTripTaxiZonesDF.show(5, truncate = false)  // Sample a few rows to inspect

      println("=== Null Check ===")
      DataQualityChecks.nullPercentages(tripsDS.toDF()).show(false)

      println("=== Range Check ===")
      DataQualityChecks.rangeChecks(tripsDS).show(false)

      println("=== Referential Integrity Check ===")
      DataQualityChecks.referentialCheck(tripsDS, zonesDS).show(false)

      println("--- TEST RUN SUMMARY START HERE ---")
        val summaryDF = RunSummary.generateSummary(tripsDS, zonesDS)
        summaryDF.show(false)
        summaryDF.write.mode("overwrite")
            .option("header", "true")
            .csv("../data/bronze/summary.csv")
        println("--- TEST RUN SUMMARY END HERE ---")

      spark.stop()
  }
}

/*

--- TO DO ---

Part B

Create reusable functions for week bucketing and other data transformations.

  - Peak-Hour Trip %: Identify congestion trends across boroughs.
  - Weekly Trip Volume by Borough: Track mobility demand trends and compare boroughs over time
  - Avg Trip Time vs Distance: Detecting congestion clusters and route inefficiencies.

  - Total Trips & Total Revenue (Weekly): Monitor revenue growth and trip counts
  - Avg Revenue per Mile: Compare efficiency across boroughs
  - Night Trip %: Balance driver scheduling and safety coverage (9PM-3AM)

Part C

Implement weekly aggregations, anomaly detection, and unit tests against small fixtures.

Write validation comparing two runs (e.g., prior week) and produce csv or parquet files.

Part D

sbt setup, scalafmt, diagrams, demo notebook/script.

Owns the final results and ensures everyone’s part integrates.

Owns visualization of KPI dashboard


Bonus

Run the spark job on GCP Dataproc or AWS EMR

*/
