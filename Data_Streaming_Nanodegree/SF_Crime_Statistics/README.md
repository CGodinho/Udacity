# SF Crime Incidents

## Introduction

Analyze SF Crime incidents with Kafka and Spark Streaming.

## Step 1

### Changed files
 + producer_server.py
 +  Kafka_server.py

**NOTE:** Topic used is "sfpd.call.log"

config server.property set:

```
port=9092
offsets.topic.replication.factor=1
```

### Run Environment

Start zookeeper and kafka:

```
/usr/bin/zookeeper-server-start config/zookeeper.properties
/usr/bin/kafka-server-start config/server.properties
```

Start producer:

```
python kafka_server.py
```
Consuming from topic with console-consumer:

```
/usr/bin/kafka-console-consumer --bootstrap-server localhost:9092 --topic sfpd.call.log --from-beginning --group "simple_tester"
```

Consuming from topic with python application (consumer_server.py):

```
python consumer_server.py
```

![consumer_output](https://github.com/CGodinho/Udacity/blob/master/Data_Streaming_Nanodegree/SF_Crime_Statistics/pics/consumer_server_output.png)


## Step 2

###Changed files
 + data_stream.py

### Run Environment

```
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.4 --master local[*] data_stream.py
```

### Spark UI

SparkContext launches its own instance of Web UI at **http://[driver]:4040**. Port is auto increased if already taken.

The port may be changed with property **spark.ui.port setting**.

**NOTE:** not possible to open a browser in workspace environment.

## Step 3


*Question: How did changing values on the SparkSession property parameters affect the throughput and latency of the data?*

As like any streaming system, as latency increases throughput is reduced. Next batch will take longer to be processed and the overall throughput decreases.


*Question: What were the 2-3 most efficient SparkSession property key/value pairs? Through testing multiple variations on values, how can you tell these were the most optimal?*

Possible properties: **spark.default.parallelism**, **spark.streaming.kafka.maxRatePerPartition** / **spark.streaming.kafka.maxRatePerPartition**

We may check results in Spark Progress Report (inputRowsPerSecond or processedRowsPerSecond) or in Spark UI Streaming page (Min, Median and Max rate of records/second).
