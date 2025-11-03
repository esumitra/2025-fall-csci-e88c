package org.cscie88c.spark

import java.sql.Timestamp
import java.time.{LocalDateTime, ZoneId}

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class SparkJobSpec extends AnyFunSuite with BeforeAndAfterAll {
	implicit var spark: SparkSession = _

	override def beforeAll(): Unit = {
		spark = SparkSession.builder()
			.appName("SparkJobSpec")
			.master("local[2]")
			.getOrCreate()
		spark.sparkContext.setLogLevel("WARN")
	}

	override def afterAll(): Unit = {
		if (spark != null) spark.stop()
	}

	private def ts(year: Int, month: Int, day: Int, hour: Int, minute: Int): Timestamp = {
		val ldt = LocalDateTime.of(year, month, day, hour, minute)
		Timestamp.from(ldt.atZone(ZoneId.systemDefault()).toInstant)
	}

	test("cleanData filters invalid rows and keeps only valid rows") {
		val s = spark
		import s.implicits._

		val rows = Seq(
			// valid row
			Row(Option(1L), ts(2025,10,10,10,0), ts(2025,10,10,10,20), Option(2L), Option(2.0), Option(1L), Option("N"), Option(100L), Option(200L), Option(1L), Option(10.0), Option(0.5), Option(0.5), Option(1.0), Option(0.0), Option(0.0), Option(12.0), Option(0.0), Option(0.0)),
			// negative fare
			Row(Option(1L), ts(2025,10,10,11,0), ts(2025,10,10,11,10), Option(1L), Option(1.0), Option(1L), Option("N"), Option(101L), Option(201L), Option(1L), Option(-5.0), Option(0.0), Option(0.0), Option(0.0), Option(0.0), Option(0.0), Option(-5.0), Option(0.0), Option(0.0)),
			// zero distance
			Row(Option(1L), ts(2025,10,10,12,0), ts(2025,10,10,12,5), Option(1L), Option(0.0), Option(1L), Option("N"), Option(102L), Option(202L), Option(1L), Option(5.0), Option(0.0), Option(0.0), Option(0.0), Option(0.0), Option(0.0), Option(5.0), Option(0.0), Option(0.0)),
			// invalid passenger_count
			Row(Option(1L), ts(2025,10,10,13,0), ts(2025,10,10,13,20), Option(9L), Option(3.0), Option(1L), Option("N"), Option(103L), Option(203L), Option(1L), Option(15.0), Option(0.0), Option(0.0), Option(0.0), Option(0.0), Option(0.0), Option(15.0), Option(0.0), Option(0.0)),
			// dropoff before pickup
			Row(Option(1L), ts(2025,10,10,14,30), ts(2025,10,10,14,0), Option(1L), Option(2.0), Option(1L), Option("N"), Option(104L), Option(204L), Option(1L), Option(8.0), Option(0.0), Option(0.0), Option(0.0), Option(0.0), Option(0.0), Option(8.0), Option(0.0), Option(0.0)),
			// negative tip
			Row(Option(1L), ts(2025,10,10,15,0), ts(2025,10,10,15,20), Option(1L), Option(2.5), Option(1L), Option("N"), Option(105L), Option(205L), Option(1L), Option(12.0), Option(0.0), Option(0.0), Option(-1.0), Option(0.0), Option(0.0), Option(12.0), Option(0.0), Option(0.0)),
			// invalid payment type
			Row(Option(1L), ts(2025,10,10,16,0), ts(2025,10,10,16,10), Option(1L), Option(1.5), Option(1L), Option("N"), Option(106L), Option(206L), Option(10L), Option(6.0), Option(0.0), Option(0.0), Option(0.0), Option(0.0), Option(0.0), Option(6.0), Option(0.0), Option(0.0))
		)

		val schema = StructType(Seq(
			StructField("VendorID", LongType, true),
			StructField("tpep_pickup_datetime", TimestampType, true),
			StructField("tpep_dropoff_datetime", TimestampType, true),
			StructField("passenger_count", LongType, true),
			StructField("trip_distance", DoubleType, true),
			StructField("RatecodeID", LongType, true),
			StructField("store_and_fwd_flag", StringType, true),
			StructField("PULocationID", LongType, true),
			StructField("DOLocationID", LongType, true),
			StructField("payment_type", LongType, true),
			StructField("fare_amount", DoubleType, true),
			StructField("extra", DoubleType, true),
			StructField("mta_tax", DoubleType, true),
			StructField("tip_amount", DoubleType, true),
			StructField("tolls_amount", DoubleType, true),
			StructField("improvement_surcharge", DoubleType, true),
			StructField("total_amount", DoubleType, true),
			StructField("congestion_surcharge", DoubleType, true),
			StructField("airport_fee", DoubleType, true)
		))

		val df = spark.createDataFrame(spark.sparkContext.parallelize(rows), schema).as[TaxiTrip]

		val cleaned = SparkJob.cleanData(df)
		val collected = cleaned.collect()

		// only the first row should be valid
		assert(collected.length == 1)
		val kept = collected.head
		assert(kept.fare_amount.contains(10.0))
		assert(kept.trip_distance.contains(2.0))
	}

	test("cleanData throws on high null rate for critical columns") {
		val s = spark
		import s.implicits._

		// Create 5 rows where 3 have null fare_amount (60% nulls) -> exceed 20% threshold
		val base = Seq(
			TaxiTrip(Some(1L), Some(ts(2025,10,10,10,0)), Some(ts(2025,10,10,10,10)), Some(1L), Some(1.0), Some(1L), Some("N"), Some(1L), Some(2L), Some(1L), Some(10.0), Some(0.0), Some(0.0), Some(1.0), Some(0.0), Some(0.0), Some(11.0), Some(0.0), Some(0.0)),
			TaxiTrip(Some(1L), Some(ts(2025,10,10,11,0)), Some(ts(2025,10,10,11,10)), Some(1L), Some(1.5), Some(1L), Some("N"), Some(1L), Some(2L), Some(1L), None, Some(0.0), Some(0.0), Some(0.0), Some(0.0), Some(0.0), Some(0.0), Some(0.0), Some(0.0)),
			TaxiTrip(Some(1L), Some(ts(2025,10,10,12,0)), Some(ts(2025,10,10,12,10)), Some(1L), Some(2.0), Some(1L), Some("N"), Some(1L), Some(2L), Some(1L), None, Some(0.0), Some(0.0), Some(0.0), Some(0.0), Some(0.0), Some(0.0), Some(0.0), Some(0.0)),
			TaxiTrip(Some(1L), Some(ts(2025,10,10,13,0)), Some(ts(2025,10,10,13,10)), Some(1L), Some(2.0), Some(1L), Some("N"), Some(1L), Some(2L), Some(1L), None, Some(0.0), Some(0.0), Some(0.0), Some(0.0), Some(0.0), Some(0.0), Some(0.0), Some(0.0)),
			TaxiTrip(Some(1L), Some(ts(2025,10,10,14,0)), Some(ts(2025,10,10,14,10)), Some(1L), Some(1.0), Some(1L), Some("N"), Some(1L), Some(2L), Some(1L), Some(8.0), Some(0.0), Some(0.0), Some(0.0), Some(0.0), Some(0.0), Some(8.0), Some(0.0), Some(0.0))
		)

		val ds = s.createDataset(base)

		val thrown = intercept[RuntimeException] {
			SparkJob.cleanData(ds)
		}

		assert(thrown.getMessage.contains("High null rates"))
	}

	test("cleanData throws when required column missing") {
	val s = spark
	import s.implicits._

		// Build rows that deliberately omit the fare_amount column from schema
		val rows = Seq(
			Row(Option(1L), ts(2025,10,10,10,0), ts(2025,10,10,10,10), Option(1L), Option(1.0), Option(1L), Option("N"))
		)

		val schema = StructType(Seq(
			StructField("VendorID", LongType, true),
			StructField("tpep_pickup_datetime", TimestampType, true),
			StructField("tpep_dropoff_datetime", TimestampType, true),
			StructField("passenger_count", LongType, true),
			StructField("trip_distance", DoubleType, true),
			StructField("RatecodeID", LongType, true),
			StructField("store_and_fwd_flag", StringType, true)
		))

	val df = s.createDataFrame(s.sparkContext.parallelize(rows), schema).as[TaxiTrip]

		val thrown = intercept[IllegalArgumentException] {
			SparkJob.cleanData(df)
		}
		assert(thrown.getMessage.contains("Missing required columns"))
	}

	test("calculateKPIs computes expected metrics and writes weekly metrics") {
	val s = spark
	import s.implicits._

		// Construct a small deterministic dataset across a single week
	val ds = s.createDataset(Seq(
			// hour 10
			TaxiTrip(Some(1L), Some(ts(2025,10,6,10,0)), Some(ts(2025,10,6,10,20)), Some(1L), Some(2.0), Some(1L), Some("N"), Some(1L), Some(2L), Some(1L), Some(10.0), Some(0.0), Some(0.0), Some(1.0), Some(0.0), Some(0.0), Some(11.0), Some(0.0), Some(0.0)),
			// hour 10
			TaxiTrip(Some(1L), Some(ts(2025,10,7,10,0)), Some(ts(2025,10,7,10,10)), Some(1L), Some(1.0), Some(1L), Some("N"), Some(1L), Some(2L), Some(1L), Some(5.5), Some(0.0), Some(0.0), Some(0.5), Some(0.0), Some(0.0), Some(5.5), Some(0.0), Some(0.0)),
			// hour 2 (night)
			TaxiTrip(Some(1L), Some(ts(2025,10,8,2,0)), Some(ts(2025,10,8,2,30)), Some(1L), Some(3.0), Some(1L), Some("N"), Some(2L), Some(3L), Some(1L), Some(20.0), Some(0.0), Some(0.0), Some(2.0), Some(0.0), Some(0.0), Some(22.0), Some(0.0), Some(0.0))
	))

		val outDir = java.nio.file.Files.createTempDirectory("kpi-test").toAbsolutePath.toString

		val kpis = SparkJob.calculateKPIs(ds, weeks = 4, outputPath = outDir)(spark)

		val collected = kpis.collect()
		// Should contain one row per borough-week; here we have two boroughs (PULocation_1 and PULocation_2)
		assert(collected.nonEmpty)

		// compute expected values manually
		val totalRevenue = 11.0 + 5.5 + 22.0
		val totalTrips = 3L
		// peak hour is 10 with 2 trips; percentage = 2/3*100 = 66.666...
		val expectedPeak = 2.0 / 3.0 * 100.0

		// avg revenue per mile = avg(total_amount / trip_distance)
		val revPerMiles = Seq(11.0/2.0, 5.5/1.0, 22.0/3.0)
		val expectedAvgRevPerMile = revPerMiles.sum / revPerMiles.size

		// avg minutes per mile = compute each: (minutes) / distance
		val minutesPerMile = Seq((20.0)/2.0, (10.0)/1.0, (30.0)/3.0) // minutes: 20,10,30
		val expectedAvgMinutesPerMile = (minutesPerMile.sum / minutesPerMile.size).toLong

		// night trips count: 1 (the hour 2 trip)
		val expectedNightCount = 1L

		// Validate that values are present in at least one KPI row (they are repeated for each borough)
		val sample = collected.head
		assert(sample.total_revenue === totalRevenue)
		assert(sample.total_trips === totalTrips)
		assert(Math.abs(sample.per_hour_trip_percentage - expectedPeak) < 0.001)
		assert(Math.abs(sample.avg_revenue_per_mile - expectedAvgRevPerMile) < 0.0001)
		assert(sample.avg_trip_time_vs_distance === expectedAvgMinutesPerMile)
		assert(sample.night_trip_percentage === expectedNightCount)

		// verify weekly metrics were written to output
		val written = java.nio.file.Paths.get(outDir, "weekly_metrics")
		assert(java.nio.file.Files.exists(written))
	}

}

