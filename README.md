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
   ```bash
   sbt compile
   sbt spark/assembly
   ```

2. Then, copy the JAR file to the `docker/apps/spark` directory:
   ```bash
   cp spark/target/scala-2.13/SparkJob.jar docker/apps/spark
   ```

3. Copy any data files to the `data` directory.

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

# Project week8
This section documents the KPIs produced by the Spark job and how to run the job locally using the provided `runSparkJob.sh` script.

### Notes on data filtering and time window
All KPI calculations operate on the filtered dataset produced by `cleanData(...)` and are computed for a recent window of data determined by the latest pickup timestamp in the input dataset. By default the Spark job uses the last 4 weeks of data (this can be changed by passing a third argument to the job). The cutoff timestamp is computed as:

cutoff_ts = max(pickup_ts) - (weeks * 7 days)

Only rows with pickup_ts >= cutoff_ts are included in the KPI calculations.

### KPIs and exact formulas
The Spark job writes a dataset with the following KPI fields. The formulas below match the code implementation in `spark/src/main/scala/org/cscie88c/spark/SparkJob.scala`.

- week_start (string)
  - The ISO week start date for the trips included in the row (computed with date_trunc("week", pickup_ts) formatted as `yyyy-MM-dd`).

- borough (string)
  - A borough placeholder derived from `PULocationID` (formatted as `PULocation_<id>`) or `UNKNOWN` when missing.

- trip_volume (Long)
  - Formula: trip_volume = COUNT(*)
  - Meaning: number of trips in that week for that borough (after cleaning and filtering by the time window).

- total_trips (Long)
  - Formula: total_trips = COUNT(*) over the same week across all boroughs
  - Meaning: total number of trips in that week (used to compute percentages).

- total_revenue (Double)
  - Formula: total_revenue = SUM(total_amount)
  - Meaning: total revenue (sum of `total_amount`) for the week across all boroughs.

- per_hour_trip_percentage (Double)
  - Code computation summary:
    1. Compute trips per pickup hour h: hour_count(h) = COUNT(*) for each hour h (0..23) over the filtered range.
    2. totalTrips = SUM_h hour_count(h)
    3. peakPercentage = (MAX_h hour_count(h) / totalTrips) * 100
  - Formula (in math form): per_hour_trip_percentage = (max_h COUNT(trips where hour=h) / totalTrips) * 100
  - Meaning: percentage of trips that occurred in the busiest (peak) hour during the filtered range. (If totalTrips == 0, this is set to 0.)

- avg_trip_time_vs_distance (Long)
  - Code computation summary:
    1. trip_minutes = (unix_timestamp(dropoff_ts) - unix_timestamp(pickup_ts)) / 60.0
    2. minutes_per_mile = trip_minutes / trip_distance  (only for rows where trip_distance > 0)
    3. avg_minutes_per_mile = AVG(minutes_per_mile)
    4. avg_trip_time_vs_distance = CAST(avg_minutes_per_mile AS Long)
  - Formula (in math form): avg_trip_time_vs_distance = CAST( AVG( ( (dropoff_ts - pickup_ts) / 60 ) / trip_distance ) AS LONG )
  - Meaning: average minutes per mile across trips (returned as a long integer). Trips with non-positive distance are excluded from this calculation.

- avg_revenue_per_mile (Double)
  - Formula: avg_revenue_per_mile = AVG( total_amount / trip_distance )  (computed only for trip_distance > 0)
  - Meaning: the average revenue per mile across trips in the filtered window.

- night_trip_percentage (Long)
  - Code computation summary: nightTripCount = COUNT(trips where hour >= 0 and hour < 6)
  - The code writes `night_trip_percentage` as this nightTripCount (i.e., a raw count), not an actual percentage.
  - Formula (as implemented): night_trip_percentage = COUNT( trips where 0 <= hour < 6 )
  - Note: despite the name `night_trip_percentage`, this value is the count of night trips. If you need an actual percentage, compute: 100 * night_trip_count / totalTrips.

### Where outputs are written
The Spark job writes two locations under the provided output path argument (outpath):

- `outpath/weekly_metrics` — parquet files containing weekly metrics grouped by `week_start` and `borough` (this is written inside `calculateKPIs(...)`).
- `outpath/kpis` — parquet files containing the final KPIs dataset (this is written by `saveOutput(...)`).

When using the included `runSparkJob.sh`, the script passes `/opt/spark-data/output/` as the `outpath`. Because the host `data/` directory is mounted into the container at `/opt/spark-data`, the resulting files will appear on the host under `data/output/kpis` and `data/output/weekly_metrics`.

### How to run the Spark job (quick steps)
1. Place your input Parquet file into the repository `data/` folder (example: `data/yellow_tripdata_2025-01.parquet`). The job expects a Parquet file with the taxi schema used in `TaxiTrip` (see `spark/src/main/scala/.../SparkJob.scala` for required columns).

2. From the repository root, make the run script executable (if needed) and run it:

```bash
chmod +x runSparkJob.sh
./runSparkJob.sh
```

The `runSparkJob.sh` script will:
- build the Spark uberjar (`sbt spark/assembly`),
- copy the jar to `docker/apps/spark`,
- start the Docker compose environment, and
- submit the Spark job inside the `spark-master` container (the script uses `/opt/spark-data/yellow_tripdata_2025-01.parquet` as the example input and `/opt/spark-data/output/` as the output directory).

3. After the job completes, inspect the output directory on the host:

- `data/output/kpis` (Parquet files with the final KPIs)
- `data/output/weekly_metrics` (Parquet files with weekly metrics by borough)

You can inspect Parquet files with tools like `parquet-tools` or by reading them with Spark/Python. Optionally, you may `docker exec` into the `spark-master` container and inspect `/opt/spark-data/output/` directly.

### Passing a custom number of weeks
The Spark job accepts a third optional argument: the number of weeks to include in KPI calculations (default is 4). To use a different number of weeks you can submit the job manually (or modify `runSparkJob.sh`) and pass a third parameter. Example (inside the container):

```bash
/opt/spark/bin/spark-submit --class org.cscie88c.spark.SparkJob --master local[*] /opt/spark-apps/SparkJob.jar /opt/spark-data/yellow_tripdata_2025-01.parquet /opt/spark-data/output/ 8
```

---

### Evidence.dev Streamlit dashboard

We added a lightweight Evidence.dev dashboard that automatically reads the Parquet outputs produced by the Spark job and provides a simple interactive view of the KPIs.

- Location (dashboard source): `docker/apps/spark/dashboard`
- Compose service: `evidence-dashboard` in `docker-compose-spark.yml`
- Environment: the service sets `EVIDENCE_ENV=evidence.dev` and mounts the host `./data` directory into the container at `/opt/spark-data`.
- Port: the dashboard listens on port `8501` (exposed from the container to the host).

What it reads
- `./data/output/kpis` — final KPIs (parquet)
- `./data/output/weekly_metrics` — weekly metrics (parquet)

Quick start (from repo root)

1. Ensure you've run the Spark job and generated the Parquet outputs (for example with `./runSparkJob.sh`). The Spark job writes to `/opt/spark-data/output` inside the containers which maps to `./data/output` on the host.

2. Build and start the dashboard with docker-compose:

```bash
docker-compose -f docker-compose-spark.yml build evidence-dashboard
docker-compose -f docker-compose-spark.yml up -d spark-master spark-worker evidence-dashboard
```

3. Open the dashboard at: http://localhost:8501

Notes
- If the dashboard reports "No parquet outputs found", verify that `data/output/kpis` and `data/output/weekly_metrics` exist on the host and contain parquet files.
- The dashboard is a Streamlit app and the source is intentionally simple; feel free to extend it with filters, additional charts, or auth as needed.


## License
Copyright 2025, Edward Sumitra

Licensed under the MIT License.