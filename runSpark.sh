sbt compile && sbt spark/assembly;
cp spark/target/scala-2.13/SparkJob.jar docker/apps/spark;
docker-compose -f docker-compose-spark.yml up -d;
sleep 5;
docker exec spark-master /opt/spark/bin/spark-submit --class org.cscie88c.spark.SparkJob --master spark://spark-master:7077 /opt/spark-apps/SparkJob.jar /opt/spark-data/yellow_tripdata_2025-01.parquet /opt/spark-data/output/;