# Running the application

## Download and building

First, you need to clone this repository and build it with [Apache Maven](https://maven.apache.org/).

```
git clone https://github.com/cerndb/metrics-monitor.git
cd metrics-monitor
mvn -DskipTests package
```

You will find the project JAR at target/metrics-monitor-VERSION.jar

## Prepare environment with Apache Spark

This project is based in [Apache Spark](https://spark.apache.org/), so you need the binaries.
You may have it installed or it can be downloaded from https://spark.apache.org/downloads.html.

The project is developed based on the version 2.11 of Spark, so we recommend to use this version.
You can download it from [here](https://archive.apache.org/dist/spark/spark-2.1.1/spark-2.1.1-bin-hadoop2.7.tgz).

## Running

As you may know, applications implemented with Spark can be run standalone, on YARN, Mesos or other platforms.

To run this applications you can use the following command:

```
$SPARK_HOME/bin/spark-submit \
			--repositories https://repository.cloudera.com/artifactory/cloudera-repos/ \
			--packages org.apache.spark:spark-streaming-kafka-0-10_2.11:2.1.0,org.reflections:reflections:0.9.9 \
			--class ch.cern.spark.metrics.Driver \
			target/metrics-monitor-VERSION.jar \
			<path_to_conf_file>
```