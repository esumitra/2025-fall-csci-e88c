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


## NYC Taxi Analytics Dashboard - Quick Start

### Prerequisites

- Node.js (v18 or later)
- Docker and Docker Compose
- Java 17
- sbt 1.9.2+

### Installation

1. **Install project dependencies:**
   ```bash
   npm install
   ```

2. **Install Evidence dependencies:**
   ```bash
   cd evidence
   npm install
   cd ..
   ```

### Running the Pipeline

**Option 1: Automated (Recommended)**

Run the complete pipeline with one command:

```bash
npm run pipeline:full
```

This will:
- Start Docker Spark containers
- Process the taxi data (Silver job)
- Calculate KPIs (Gold job)
- Update the dashboard data
- Start the Evidence development server

**Option 2: Manual Steps**

If you prefer to run each step individually:

1. Start Docker containers:
   ```bash
   npm run docker:up
   ```

2. Run data processing:
   ```bash
   npm run spark:silver
   npm run spark:gold
   ```

3. Update dashboard database:
   ```bash
   npm run update:duckdb
   ```

4. Start the dashboard:
   ```bash
   npm run dev
   ```

### View the Dashboard

Open your browser to:
```
http://localhost:3000
```

### Stop Everything

Stop Docker containers:
```bash
npm run docker:down
```

Stop Evidence (press Ctrl+C in the terminal running the dev server)

### Troubleshooting

**Dashboard shows no data:**
- Make sure Spark jobs completed successfully
- Run `npm run update:duckdb` to refresh the database
- Restart the Evidence server

**Docker errors:**
- Verify Docker is running: `docker ps`
- Check logs: `docker logs spark-master`

**Build errors:**
- Ensure Java 17 is installed: `java -version`
- Clean build: `sbt clean`
npm 


# Doing things the old way
## Running the Spark Job in Docker
1. First, ensure that your Spark application uberjar file is built. You can build the Spark uberjar file by using the following sbt commands:

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

3. Copy any data files to the `data` directory. These files are required to be in these locations.
``` 
data/yellow_tripdata_2025-01.parquet
data/taxi_zone_lookup.csv
```


4. Next, start the Docker container:
   ```bash
   docker compose -f docker-compose-spark.yml up -d
   ```
5. Finally, submit the Spark jobs: Note that Silver must run before Gold.
   ```bash
   docker exec -it spark-master /bin/bash
   
   /opt/spark/bin/spark-submit --class org.cscie88c.spark.BronzeJob --master spark://spark-master:7077 /opt/spark-apps/SparkJob.jar
    /opt/spark/bin/spark-submit --class org.cscie88c.spark.SilverJob --master spark://spark-master:7077 /opt/spark-apps/SparkJob.jar
    /opt/spark/bin/spark-submit --class org.cscie88c.spark.GoldJob --master spark://spark-master:7077 /opt/spark-apps/SparkJob.jar
   ```
6. To stop the Docker containers,

   Exit the container shell and run:
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



