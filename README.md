## Programming in Scala for Big Data Systems, Fall 2025
Scala Project for Harvard Extension course CSCI E-88C, Fall, 2025. See course details at [Scala for Big Data](https://courses.dce.harvard.edu/?details&srcdb=202601&crn=16769).


This project is a multi-module setup for Scala applications that integrate with big data frameworks like Spark, Beam, and Kafka. It is designed to facilitate development in a structured manner, allowing for modular code organization and easy dependency management.

The project requires Java 17, Scala 2.13 and sbt 1.9.2+ environment to run.

## Project Structure
- **core**: Contains the core library code shared across other modules.
- **cli**: Implements the command-line interface for the application.
- **spark**: Contains Spark-specific code and configurations.
- **build.sbt**: The main build file for the project, defining the modules and their dependencies.
- **README.md**: This file, providing an overview of the project and its structure.

## Build Tool
This project uses [SBT](https://www.scala-sbt.org/) (Scala Build Tool) for building and managing dependencies. SBT allows for incremental compilation and easy management of multi-module projects.

## Getting Started
1. **Clone the repository**:
   ```bash
   git clone https://github.com/esumitra/2025-fall-csci-e88c.git

   cd 2025-fall-csci-e88c
   ```
2. **Build the project**:
   ```bash
   sbt compile
   ```
3. **Run the CLI**:
   ```bash
   sbt "cli/run John"
   ```

   To packge an executable file use the native packager plugin:
   ```bash
   sbt "cli/stage"
    ```
    This will create a script in the `cli/target/universal/stage/bin` directory that you can run directly.

   e.g., ` ./cli/target/universal/stage/bin/cli John`
4. **Run the Spark application**:
   ```bash
   sbt spark/run
   ```

## Running in Codespaces
This project is configured to run in GitHub Codespaces, providing a ready-to-use development environment. Click the "Open in Github Codespaces" button below to start the developer IDE in the cloud.

[![Open in GitHub Codespaces](https://github.com/codespaces/badge.svg)](https://codespaces.new/esumitra/2025-fall-csci-e88c?quickstart=1)


## Running in DevContainer
This project can be run in a DevContainer for a consistent development environment. Ensure you have [Visual Studio Code](https://code.visualstudio.com/) and the [Remote - Containers extension](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers) installed. Follow these steps:
1. Open the project in Visual Studio Code.
2. Press `(Cmd+Shift+P)` and select `Dev Containers: Reopen in Container`.
3. Wait for the container to build and start.
4. Open a terminal in the DevContainer and run the project using SBT commands as described above.

## Running Spark

1. Start the docker container:
   ```bash
   docker compose -f docker-compose-spark.yml up
   ```
2. Connect to the spark-master:
   ```bash
   docker exec -it spark-master /bin/bash
   ```
3. Run the spark shell interactively:
   ```bash
   /opt/spark/bin/spark-shell
   ```

4. Run the Spark application:

   Submit the Spark job using the `/opt/spark/bin/spark-submit` command.

## Running the Spark Job in Docker
1. First, ensure that your Spark application uberjar file is built. You can build the Spark uberjar file by using the following sbt commands:
   (Zach note: second command takes FOREVER - 336s-412s on my dev machine)
2. Further Zach note, I've managed to knock this down a bit if you enter the SBT shell and increase the RAM available to SBT in spark/build.sbt. I used:
3. javaOptions ++= Seq("-Xms512M", "-Xmx16G", "-XX:+CMSClassUnloadingEnabled"), which is in the repo now and allows the jvm to use up to 16GB of RAM to compile things
4. Also enter the sbt shell first, which keeps some of it a little bit warm and knocks it down to a still unreasonable but slightly less mind numbing 2:30
   ```bash
   sbt 
   compile
   spark/assembly
   exit
   ```

2. Then, copy the JAR file to the `docker/apps/spark` directory:
   ```bash
   cp spark/target/scala-2.13/SparkJob.jar docker/apps/spark
   ```

3. Copy any data files to the `data` directory. (Zach note, I stuck the taxi_zone_lookup.csv in there in the repo, make sure you download the yellow_tripdata_2025-01.parquet file, which isn't included because it's biggish)

   The `data` directory is mounted to the Docker container at `/opt/spark-data`. Ensure that any input data files required by your Spark job are placed in this directory.

4. Next, start the Docker container:
   ```bash
   docker compose -f docker-compose-spark.yml up -d
   ```
5. Finally, submit the Spark job:
   ```bash
   docker exec -it spark-master /bin/bash
   
   /opt/spark/bin/spark-submit --class org.cscie88c.spark.SparkJob --master spark://spark-master:7077 /opt/spark-apps/SparkJob.jar
   ```
6. To stop the Docker containers,

   Exit the container shell and run:
   ```bash
   docker compose -f docker-compose-spark.yml down
   ```

## Docker commands

1. Start the docker container:
   ```bash
   docker compose -f docker-compose-spark.yml up
   ```
2. Check status of running containers:
   ```bash
   docker ps
   ```
3. Review the logs:
   ```bash
   docker logs -f spark-master
   ```
4. Stop running containers:
   ```bash
   docker compose -f docker-compose-spark.yml down
   ```

### Static Analysis Tools

#### Scalafmt
To ensure clean code, run scalafmt periodically. The scalafmt configuration is defined at https://scalameta.org/scalafmt/docs/configuration.html

For source files,

`sbt scalafmt`

For test files.

`sbt test:scalafmt`

#### Scalafix
To ensure clean code, run scalafix periodically. The scalafix rules are listed at https://scalacenter.github.io/scalafix/docs/rules/overview.html

For source files,

`sbt "scalafix RemoveUnused"`

For test files.

`sbt "test:scalafix RemoveUnused"`

## License
Copyright 2025, Edward Sumitra

Licensed under the MIT License.

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

Overview:
---------
This Spark job (SilverJob.scala) cleans NYC Yellow Taxi trip data by applying
multiple data quality (DQ) rules sequentially. It removes invalid or null records,
logs metrics for each step, and writes both cleaned data and rejected rows.

Inputs:
-------
/opt/spark-data/yellow_tripdata_2025-01.parquet   → Raw trip data
/opt/spark-data/taxi_zone_lookup.csv              → Zone info (not used yet)

Outputs:
--------
/opt/spark-data/silver/trips/                     → Final cleaned Parquet
/opt/spark-data/silver/rejects_<rule_name>/       → Rejected rows (CSV)
/opt/spark-data/silver/clean_sample_500k/         → 500K-row sample (CSV)

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
- All output paths configurable via SilverRoot variable.
- Modify or add rules using the helper:
    applyDQRule(currentDF, "rule_name", <condition>)

