package org.cscie88c

import org.apache.spark.sql.{DataFrame}
import org.apache.spark.sql.functions._


object AnalysisFunctions {

    // Peak Hours defined: 7 AM - 9 AM (7, 8, 9) and 4 PM - 6 PM (16, 17, 18).

    def getPeakHourTripPercentage(df: DataFrame, targetBorough: String): (String, String) = {
        val peakHours = Seq(7, 8, 9, 16, 17, 18)

        val boroughDF = df.filter(col("BoroughPU") === targetBorough) // Filter for the target borough

        val totalTrips = boroughDF.count()

        if (totalTrips == 0) {
            (targetBorough, "0.00%")
        } else {
            val peakTrips = boroughDF
                .filter(col("Pickup_Hour").isInCollection(peakHours)) // Filter for peak hours
                .count()

            val percentage = (peakTrips.toDouble / totalTrips.toDouble) * 100
            (targetBorough, f"$percentage%.2f%%")
        }
    }


    // Calculates the total trip volume for a specific borough across the entire dataset.

    //def getWeeklyTripVolumeByBorough(df: DataFrame, targetBorough: String): (String, Long) = {
    //    val totalTrips = df.filter(col("BoroughPU") === targetBorough).count() // Total trips in the target borough
    //    (targetBorough, totalTrips)
    //}
    
    // Calculates the weekly trip volume for a all borough across the entire dataset.
    def getTripVolumeByWeekForAllBoroughs(df: DataFrame, targetWeek: Int): DataFrame = {
        println(s"\n--- Total Trip Volume for Week $targetWeek by Borough ---")
        df.filter(col("Pickup_Week") === targetWeek)
            .groupBy(col("BoroughPU"))
            .agg(
                count("*").as("Total_Trips")
            )
            .withColumnRenamed("BoroughPU", "Borough")
    }


    // Calculates the Total Trips and Total Revenue for a specific week and borough.

    def getTotalTripsAndRevenueByWeek(df: DataFrame, targetWeek: Int, targetBorough: String): DataFrame = {
        println(s"\n--- Total Trips & Total Revenue for Week $targetWeek in $targetBorough ---")
        df.filter(col("Pickup_Week") === targetWeek)
            .filter(col("BoroughPU") === targetBorough)
            .groupBy(col("Pickup_Week"), col("BoroughPU"))
            .agg(
                count("*").as("Total_Trips"),
                sum(col("fare_amount")).as("Total_Revenue")
            )
            .withColumnRenamed("BoroughPU", "Borough")
    }


    // Compares operational efficiency across all boroughs by calculating Average Revenue per Mile.
    // Avg Revenue per Mile = SUM(fare_amount) / SUM(distance)

    def compareEfficiencyAcrossBoroughs(df: DataFrame): DataFrame = {
        println("\n--- Average Revenue Per Mile by Borough (Efficiency Comparison) ---")
        df.filter(col("trip_distance") > 0) // Filter out zero-distance trips which distort the metric
            .groupBy(col("BoroughPU"))
            .agg(
                sum(col("fare_amount")).as("Total_Revenue"),
                sum(col("trip_distance")).as("Total_Distance")
            )
            .withColumn(
                "Avg_Revenue_Per_Mile",
                col("Total_Revenue") / col("Total_Distance")
            )
            .select(
                col("BoroughPU").as("Borough"),
                format_number(col("Avg_Revenue_Per_Mile"), 2).as("Avg_Revenue_Per_Mile")
            )
            .orderBy(desc("Avg_Revenue_Per_Mile"))
    }

    // night Hours defined: 9 PM - 3 AM (21, 22, 23, 0, 1, 2, 3).
    def getNightTripPercentage(df: DataFrame, targetBorough: String): (String, String) = {
        val nightHours = Seq(21, 22, 23, 0, 1, 2, 3)

        val boroughDF = df.filter(col("BoroughPU") === targetBorough) // Filter for the target borough

        val totalTrips = boroughDF.count()

        if (totalTrips == 0) {
            (targetBorough, "0.00%")
        } else {
            val nightTrips = boroughDF
                .filter(col("Pickup_Hour").isInCollection(nightHours)) // Filter for night hours
                .count()

            val percentage = (nightTrips.toDouble / totalTrips.toDouble) * 100
            (targetBorough, f"$percentage%.2f%%")
        }
    }
}
