package org.cscie88c.spark

import org.scalatest.funsuite.AnyFunSuite
import java.time._

class TransformUtilsTest extends AnyFunSuite {

  // ----------------------------------------------------------
  // Helper: classify hour of day into period labels
  // ----------------------------------------------------------
  def classifyHour(hour: Int): String =
    if (hour >= 5 && hour <= 11) "Morning"
    else if (hour >= 12 && hour <= 17) "Afternoon"
    else if (hour >= 18 && hour <= 22) "Evening"
    else "Night"

  // ----------------------------------------------------------
  // Test 1: day period classification
  // ----------------------------------------------------------
  test("classifyHour should correctly map hour ranges to day periods") {
    assert(classifyHour(6) == "Morning")
    assert(classifyHour(10) == "Morning")
    assert(classifyHour(13) == "Afternoon")
    assert(classifyHour(19) == "Evening")
    assert(classifyHour(23) == "Night")
    assert(classifyHour(3) == "Night")
  }

  // ----------------------------------------------------------
  // Helper: get start of week (Monday)
  // ----------------------------------------------------------
  def weekStart(date: LocalDate): LocalDate =
    date.minusDays(date.getDayOfWeek.getValue - 1)

  // ----------------------------------------------------------
  // Test 2: weekStart should always return a Monday
  // ----------------------------------------------------------
  test("weekStart should always return the Monday of the same week") {
    val date = LocalDate.of(2025, 1, 9) // Thursday
    val monday = weekStart(date)
    assert(monday.getDayOfWeek == DayOfWeek.MONDAY)
    assert(monday.isBefore(date))
    assert(monday.toString == "2025-01-06")
  }

  // ----------------------------------------------------------
  // Helper: calculate trip duration (minutes)
  // ----------------------------------------------------------
  def tripDurationMinutes(pickup: LocalDateTime, dropoff: LocalDateTime): Double =
    java.time.Duration.between(pickup, dropoff).toMinutes.toDouble

  // ----------------------------------------------------------
  // Test 3: duration calculation
  // ----------------------------------------------------------
  test("tripDurationMinutes should compute positive duration in minutes") {
    val pickup = LocalDateTime.of(2025, 1, 10, 14, 0)
    val dropoff = LocalDateTime.of(2025, 1, 10, 14, 45)
    val duration = tripDurationMinutes(pickup, dropoff)
    assert(duration == 45.0)
  }

  test("tripDurationMinutes should return negative for reversed timestamps") {
    val pickup = LocalDateTime.of(2025, 1, 10, 14, 45)
    val dropoff = LocalDateTime.of(2025, 1, 10, 14, 0)
    val duration = tripDurationMinutes(pickup, dropoff)
    assert(duration == -45.0)
  }

  // ----------------------------------------------------------
  // Helper: validate reasonable trip durations (1–180 min)
  // ----------------------------------------------------------
  def isReasonableDuration(mins: Double): Boolean =
    mins >= 1.0 && mins <= 180.0

  // ----------------------------------------------------------
  // Test 4: reasonable trip duration logic
  // ----------------------------------------------------------
  test("isReasonableDuration should return true for valid range and false otherwise") {
    assert(isReasonableDuration(10.0))
    assert(isReasonableDuration(1.0))
    assert(isReasonableDuration(180.0))
    assert(!isReasonableDuration(0.5))
    assert(!isReasonableDuration(250.0))
  }
}

