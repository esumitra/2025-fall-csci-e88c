package org.cscie88c

//Gold Layer - Testable KPI Functions (No Spark)
 
object GoldFunctionsTestable {

  case class TripRecord(
    borough: String,
    pickupHour: Int,
    pickupWeek: Int,
    fareAmount: Double,
    tripDistance: Double
  )

  // KPI 1: Peak Hour Trip Percentage
  def calculatePeakHourPercentage(trips: List[TripRecord], borough: String): Double = {
    val peakHours = Set(7, 8, 9, 17, 18, 19)
    val boroughTrips = trips.filter(_.borough == borough)
    
    if (boroughTrips.isEmpty) 0.0
    else {
      val peakTrips = boroughTrips.count(t => peakHours.contains(t.pickupHour))
      (peakTrips.toDouble / boroughTrips.length) * 100
    }
  }

  // KPI 2: Weekly Trip Volume
  def calculateWeeklyTripVolume(trips: List[TripRecord], week: Int, borough: String): Long = {
    trips.count(t => t.pickupWeek == week && t.borough == borough)
  }

  // KPI 3: Total Revenue
  def calculateWeeklyRevenue(trips: List[TripRecord], week: Int, borough: String): Double = {
    trips.filter(t => t.pickupWeek == week && t.borough == borough)
         .map(_.fareAmount)
         .sum
  }

  // KPI 4: Revenue per Mile (Efficiency)
  def calculateRevenuePerMile(trips: List[TripRecord], borough: String): Double = {
    val boroughTrips = trips.filter(_.borough == borough).filter(_.tripDistance > 0)
    
    if (boroughTrips.isEmpty) 0.0
    else boroughTrips.map(_.fareAmount).sum / boroughTrips.map(_.tripDistance).sum
  }

  // KPI 5: Night Trip Percentage
  def calculateNightTripPercentage(trips: List[TripRecord], borough: String): Double = {
    val nightHours = Set(21, 22, 23, 0, 1, 2, 3)
    val boroughTrips = trips.filter(_.borough == borough)
    
    if (boroughTrips.isEmpty) 0.0
    else {
      val nightTrips = boroughTrips.count(t => nightHours.contains(t.pickupHour))
      (nightTrips.toDouble / boroughTrips.length) * 100
    }
  }

  // Transform: Filter by week
  def filterByWeek(trips: List[TripRecord], week: Int): List[TripRecord] = {
    trips.filter(_.pickupWeek == week)
  }

  // Transform: Filter by borough
  def filterByBorough(trips: List[TripRecord], borough: String): List[TripRecord] = {
    trips.filter(_.borough == borough)
  }

  // Dataset-level check: Total trips
  def totalTrips(trips: List[TripRecord]): Long = trips.length

  // Dataset-level check: Validate revenue
  def validateRevenue(trips: List[TripRecord]): (Long, Long) = {
    val positive = trips.count(_.fareAmount > 0)
    val negative = trips.count(_.fareAmount <= 0)
    (positive, negative)
  }
}