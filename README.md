# SparkConn: Building Spark Connectors

## Motivation
Apache Spark has become the preferred tool for ETL, machine-learning and distributed 
data processing in the Hadoop and big data ecosystem for several reasons. 
First, it offers a unifying, consistent, easy-to-use REPL-based interactive and 
programmatic environment for all those kinds of processing. 
Second, the Spark ecosystem has built-in as well as third-party connectors to pull 
and push data from a variety of batch and streaming data sources. 
And third, there is a large and helpful userbase and ecosystem along with several resources.

However there are occasions where a connector for a specific data source
is not available or an existing connector is not suitable and needs considerable 
modification to fit a use case.

This is a very simple tutorial ()working examples) to help understand how batch and streaming 
data source and sink connectors work. The intent is that the examples will help developers 
build new connnectors as well as customize existing ones and hopefully contribute back to the Spark community.

## Batch v/s Streaming
Note that starting with Spark 2.3, there is the notion of batch, microbatch streaming and continuous streaming 
data sources. Batch is essentially reading data from a static, non-mutating data source 
(non-mutating for the duration of the data extract). 

Microbatch streaming or simply microbatch is the notion that was introduced in Spark v1.6 where data is retrieved from a 
data source at short periodic intervals (seconds to minutes). 
Like RDD v/s Dataset, the initial notion of streaming, known as dstream was not strongly typed. 
And Spark 2.0 introduced structured streaming that is strongly typed and has recoverability/restartability capabilities.

Continuous streaming is the notion that has been introduced with Spark 2.3 that has low latency and I think is still evolving.

A key difference between microbatch and continuous from an architecture perspective is how the low latency is achieved.
In microbatch, the actual data generating class is instantiated on the driver at each processing interval for all partitions
and distributed to the executors at each trigger interval. In continuous streaming source, the class is instantiated only once
for each partition.

Also, each trigger or processing interval is referred to as epoch in the API documentation.

## Source and Sink V2 API
Spark 2.3 is a watershed moment as it introduced the notions of microbatch v/s continuous streaming 
and the initial version of the evolving V2 Source and Sink API. Prior to this version, writing your own
custom data source and sink was non-trivial and requiring non-trivial effort and understanding of some 
Spark internals/code. And developing a streaming source required you to create your package as a sub-package
of org.apache.spark.sql as certain methods were not exposed otherwise.

V2 API made developing sources and sinks quite simple and requires implementing a few simple and consistent 
APIs (Java interfaces). Note that this is still an evolving interface and there is the potential of some 
changes to the API, but it is hoped that the changes would not be very disruptive or may be downward compatible.

Below are some reference sources that provide additional details on RDDs and structured streaming.

[Matei Zahari's Original Spark Paper](http://people.csail.mit.edu/matei/papers/2012/nsdi_spark.pdf)

[Matei Zahari's PhD Thesis on Spark](https://www2.eecs.berkeley.edu/Pubs/TechRpts/2011/EECS-2011-82.pdf)

[Introduction to Structured Streaming (Video) - Part 1](https://databricks.com/session/easy-scalable-fault-tolerant-stream-processing-with-structured-streaming-in-apache-spark)

[Introduction to Structured Streaming (Video) - Part 2](https://databricks.com/session/easy-scalable-fault-tolerant-stream-processing-with-structured-streaming-in-apache-spark-continues)

[RDD Chapter from Jacek Laskowski's Online Git-Book on Spark](https://jaceklaskowski.gitbooks.io/mastering-apache-spark/content/spark-rdd.html)

[Jacek Laskowski's Online Git-Book on Spark Streaming](https://jaceklaskowski.gitbooks.io/spark-structured-streaming/)

[Continuous Streaming as Introduced in Spark 2.3](https://databricks.com/blog/2018/03/20/low-latency-continuous-processing-mode-in-structured-streaming-in-apache-spark-2-3-0.html)

[MongoDB Spark Driver Presentation @ Spark Summit 2016](https://www.youtube.com/watch?v=O9kpduk5D48)

# Sample Datasource and Datasink Connectors

This project contains very trivial data source connectors that generate a simple string. 
The batch and streaming datasinks simply print the input data to the console with some additional diagnostic information.

# Build and Test

The project can be build using `sbt assembly` and tested using spark-shell as follows:

## Batch Source and Sink
```
spark-shell --jars target/scala-2.11/sparkconn-assembly-0.1.jar
....

val mydata1 = spark.read.format("V2BatchDataSource").load()
mydata1.printSchema
mydata1.show(100, false)

val mydata2 = spark.read.format("V2BatchDataSource").
option("partitions", "1").option("rowsperpartition", "1").
load()
mydata2.show(10, false)

mydata1.write.format("V2BatchDataSink").save

mydata1.write.format("V2BatchDataSink").mode("append").save

val mydata3 = spark.createDataset(1 to 10)
mydata3.write.format("V2BatchDataSink").save

```


## MicroBatch Streaming Datasource
```
spark-shell --jars target/scala-2.11/sparkconn-assembly-0.1.jar
....

import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.streaming.ProcessingTime

val data = spark.readStream.format("V2MicroBatchDataSource").
option("partitions", "3").option("rowsperpartition", "10").load()

val query = data.writeStream.outputMode("append").
format("console").option("truncate", "false").trigger(Trigger.Once).start()

val query = data.writeStream.outputMode("append").
format("console").option("truncate", "false").
option("numRows", "100").trigger(ProcessingTime("1 second")).start()

Thread.sleep(5000)

query.stop()

```

## Continuous Streaming Datasource
```

import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.streaming.ProcessingTime

val data = spark.readStream.format("V2ContinuousDataSource").load()

val query = data.writeStream.outputMode("append").
            format("console").option("truncate", "false").
            trigger(Trigger.Once).start()

val query = data.writeStream.outputMode("append").
format("console").option("truncate", "false").
trigger(Trigger.Continuous("1 second")).start()

Thread.sleep(5000)

query.stop

```
