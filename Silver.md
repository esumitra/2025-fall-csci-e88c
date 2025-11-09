SilverJob - Trip Data Cleaning & Transformation

Overview:
---------
This Spark job (SilverJob.scala) performs cleaning and transformation of NYC Yellow Taxi trip data.
It applies sequential data quality (DQ) rules, removes invalid and null records, standardizes timestamps
to EST, joins with Taxi Zone Lookup data, and adds weekly bucketing for aggregation readiness.
The job logs detailed metrics, writes both cleaned and conformed outputs, and saves sample CSVs and reject files.

Inputs:
-------
/opt/spark-data/yellow_tripdata_2025-01.parquet   → Raw trip data  
/opt/spark-data/taxi_zone_lookup.csv              → Taxi zone lookup data

Outputs:
--------
/opt/spark-data/silver/trips/                     → Cleaned Parquet (post-DQ)  
/opt/spark-data/silver/trips_conformed/           → Conformed Parquet (joined + EST + weekly)  
/opt/spark-data/silver/rejects_<rule_name>/       → Rejected rows (CSV)  
/opt/spark-data/silver/clean_sample_10k/          → 10K-row cleaned sample (CSV)  
/opt/spark-data/silver/conformed_sample_10k/      → 10K-row conformed sample (CSV)
SilverJob - Trip Data Cleaning

Run Instructions:
-----------------
From project root:
sbt "spark/runMain org.cscie88c.spark.SilverJob"

Or build and run manually:
sbt "spark/assembly"
spark-submit --class org.cscie88c.spark.SilverJob \
--master local[*] spark/target/scala-2.13/spark-assembly-*.jar

Expected runtime: ~1–3 min (on ~3.5M rows)

Applied DQ Rules (in order):
----------------------------
1. no_null_values              → remove any rows with nulls
2. passenger_count_valid       → passenger_count ∈ [1, 8]
3. total_amount_nonnegative    → total_amount ≥ 0
4. chronological_order_valid   → dropoff ≥ pickup
5. duration_reasonable         → trip_duration_min ∈ [1, 180]
6. distance_reasonable         → trip_distance ∈ [0.1, 100]
7. payment_type_valid          → payment_type ∈ [1, 6]
8. ratecode_valid              → RatecodeID ∈ [1, 6]
9. pickup_dropoff_not_null     → ensure timestamps exist
10. no_negative_values         → remove any negative numeric fields

Each rule prints metrics and writes rejects:
=== METRICS: rule_name ===
Total before:  3,475,226
Kept rows:     3,100,000
Rejected rows:   375,226
Rejects written to: /opt/spark-data/silver/rejects_rule_name/

Final Outputs:
--------------
- Cleaned data:  /opt/spark-data/silver/trips/clean_trips_2025_01.parquet
- Sample CSV:    /opt/spark-data/silver/clean_sample_500k/
- Reject files:  /opt/spark-data/silver/rejects_<rule_name>/

Verification Tips:
------------------
- Check Spark logs for metrics at each stage.
- Validate record counts: kept + rejected = total before.
- To preview data:
  spark.read.parquet("/opt/spark-data/silver/trips").show(20, false)
- To inspect rejects:
  head /opt/spark-data/silver/rejects_duration_reasonable/part-*.csv

Notes:
------
- Each run overwrites previous results (repeatable).
- All output paths configurable via SilverRoot variable in the Utilities object
- Modify or add rules using the helper:
  applyDQRule(currentDF, "rule_name", <condition>, outputRejects)