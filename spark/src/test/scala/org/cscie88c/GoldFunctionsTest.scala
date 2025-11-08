package org.cscie88c

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers
import org.cscie88c.GoldFunctionsTestable._

//Gold Layer Unit Tests - KPIs and Transforms

class GoldFunctionsTest extends AnyWordSpec with Matchers {

  val testTrips: List[TripRecord] = List(
    TripRecord("Manhattan", 8, 1, 15.0, 2.5),
    TripRecord("Manhattan", 17, 1, 20.0, 3.0),
    TripRecord("Manhattan", 12, 1, 10.0, 1.5),
    TripRecord("Manhattan", 22, 1, 25.0, 5.0),
    TripRecord("Manhattan", 0, 1, 30.0, 4.0),
    TripRecord("Queens", 8, 1, 12.0, 2.0),
    TripRecord("Manhattan", 9, 2, 18.0, 3.5),
    TripRecord("Manhattan", 0, 2, 28.0, 6.0)
  )

  // ===== KPI Tests =====
  "KPI: Peak Hour Trip Percentage" should {
    "calculate correct percentage for Manhattan" in {
      val result = calculatePeakHourPercentage(testTrips, "Manhattan")
      result shouldBe 42.86 +- 0.1 // 3 out of 7 Manhattan trips (hours 8, 17, 9)
    }

    "return 0 for empty borough" in {
      val result = calculatePeakHourPercentage(testTrips, "Bronx")
      result shouldBe 0.0
    }
  }

  "KPI: Weekly Trip Volume" should {
    "count Manhattan week 1 trips" in {
      val result = calculateWeeklyTripVolume(testTrips, 1, "Manhattan")
      result shouldBe 5 // 5 Manhattan trips in week 1
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
      result shouldBe 100.0 +- 0.01 // 15+20+10+25+30
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
      result shouldBe 42.86 +- 0.1 // 3 out of 7 Manhattan trips (hours 22, 0, 0)
    }

    "return 0 for Queens with no night trips" in {
      val result = calculateNightTripPercentage(testTrips, "Queens")
      result shouldBe 0.0
    }
  }

  // ===== Transform Tests =====
  "Transform: Filter by week" should {
    "return only week 1 trips" in {
      val result = filterByWeek(testTrips, 1)
      result.length shouldBe 6 // 5 Manhattan + 1 Queens
      result.foreach(_.pickupWeek shouldBe 1)
    }
  }

  "Transform: Filter by borough" should {
    "return only Manhattan trips" in {
      val result = filterByBorough(testTrips, "Manhattan")
      result.length shouldBe 7 // 7 Manhattan trips total
      result.foreach(_.borough shouldBe "Manhattan")
    }
  }

  // ===== Dataset-Level Checks =====
  "Dataset Check: Total trips" should {
    "count all trips" in {
      val result = totalTrips(testTrips)
      result shouldBe 8 // Total of 8 trips
    }
  }

  "Dataset Check: Revenue validation" should {
    "count positive and negative fares" in {
      val (positive, negative) = validateRevenue(testTrips)
      positive shouldBe 8 // All 8 trips have positive fares
      negative shouldBe 0
    }
  }
}