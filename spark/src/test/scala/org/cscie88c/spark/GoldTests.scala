package org.cscie88c

import org.cscie88c.core.testutils.StandardTest
import org.cscie88c.GoldFunctions._

//Gold Layer Unit Tests - KPIs and Transforms

class GoldFunctionsTest extends StandardTest {

  val testTrips: List[TripRecord] = List(
    TripRecord("Manhattan", 8, 1, 15.0, 2.5), // Week 1, peak
    TripRecord("Manhattan", 16, 1, 20.0, 3.0), // Week 1, peak
    TripRecord("Manhattan", 12, 1, 10.0, 1.5), // Week 1, off-peak
    TripRecord("Manhattan", 22, 1, 25.0, 5.0), // Week 1, night
    TripRecord("Queens", 8, 1, 12.0, 2.0), // Week 1, peak
    TripRecord("Manhattan", 9, 2, 18.0, 3.5), // Week 2, peak
    TripRecord("Manhattan", 0, 2, 28.0, 6.0) // Week 2, night
  )

  // KPI Tests
  "KPI: Peak Hour Trip Percentage" should {
    "calculate correct percentage for Manhattan" in {
      val result = calculatePeakHourPercentage(testTrips, "Manhattan")
      result shouldBe 60.0 +- 0.1 // 3 out of 5 Manhattan trips
    }

    "return 0 for empty borough" in {
      val result = calculatePeakHourPercentage(testTrips, "Bronx")
      result shouldBe 0.0
    }
  }

  "KPI: Weekly Trip Volume" should {
    "count Manhattan week 1 trips" in {
      val result = calculateWeeklyTripVolume(testTrips, 1, "Manhattan")
      result shouldBe 4
    }

    "count Manhattan week 2 trips" in {
      val result = calculateWeeklyTripVolume(testTrips, 2, "Manhattan")
      result shouldBe 2
    }

    "return 0 for non-existent week" in {
      val result = calculateWeeklyTripVolume(testTrips, 5, "Manhattan")
      result shouldBe 0
    }
  }

  "KPI: Weekly Revenue" should {
    "calculate Manhattan week 1 revenue" in {
      val result = calculateWeeklyRevenue(testTrips, 1, "Manhattan")
      result shouldBe 70.0 +- 0.01 // 15+20+10+25
    }

    "calculate Manhattan week 2 revenue" in {
      val result = calculateWeeklyRevenue(testTrips, 2, "Manhattan")
      result shouldBe 46.0 +- 0.01 // 18+28
    }
  }

  "KPI: Revenue per Mile" should {
    "calculate Manhattan efficiency" in {
      val result = calculateRevenuePerMile(testTrips, "Manhattan")
      result should be > 0.0
      result should be < 20.0
    }

    "return 0 for empty borough" in {
      val result = calculateRevenuePerMile(testTrips, "Bronx")
      result shouldBe 0.0
    }
  }

  "KPI: Night Trip Percentage" should {
    "calculate correct percentage for Manhattan" in {
      val result = calculateNightTripPercentage(testTrips, "Manhattan")
      result shouldBe 40.0 +- 0.1 // 2 out of 5 Manhattan trips
    }

    "return 0 for Queens with no night trips" in {
      val result = calculateNightTripPercentage(testTrips, "Queens")
      result shouldBe 0.0
    }
  }

  // Transform Tests
  "Transform: Filter by week" should {
    "return only week 1 trips" in {
      val result = filterByWeek(testTrips, 1)
      result.length shouldBe 5
      result.foreach(_.pickupWeek shouldBe 1)
    }
  }

  "Transform: Filter by borough" should {
    "return only Manhattan trips" in {
      val result = filterByBorough(testTrips, "Manhattan")
      result.length shouldBe 5
      result.foreach(_.borough shouldBe "Manhattan")
    }
  }

  // Dataset-Level Checks
  "Dataset Check: Total trips" should {
    "count all trips" in {
      val result = totalTrips(testTrips)
      result shouldBe 7
    }
  }

  "Dataset Check: Revenue validation" should {
    "count positive and negative fares" in {
      val (positive, negative) = validateRevenue(testTrips)
      positive shouldBe 7
      negative shouldBe 0
    }
  }
}