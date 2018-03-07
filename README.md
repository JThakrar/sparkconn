# SparkConn: Building Spark Connectors

## Motivation
Apache Spark has become the preferred tool for ETL, machine-learning and distributed 
data processing in general in the Hadoop and big data ecosystem for several reasons. 
First, it offers a unifying, consistent, easy-to-use REPL-based interactive and 
programmatic environment for all those kinds of processing. 
Second, the Spark ecosystem has built-in as well as third-party connectors to pull 
and push data from a variety of batch (or static) and streaming data sources.

However there are occasions where a connector for a specific data source
is not available or an existing connector is not suitable and needs considerable 
modification to fit the use case.

This is a very simple tutorial and working example to help understand how batch and streaming 
data source connectors work.

## Details
For the purpose of this tutorial, a data source that just produces a simple string based dataset
for both batch and streaming datastream. Note that the notion of distrubted dataset in Spark has
been evolving - starting with RDD with the initial version and no strong notion of a 
schema for the distributed dataset, then evolving to DataFrame with schema,
followed by Dataset which are strongly typed. On the streaming front, the idea was introduced
using dstream (spark streaming), followed by structured streaming which introduced once-only 
semantics and recoverability with certain assumptions on the source and sink side. 
As of this writing (March 2018), Beginning Spark 2.3 (released Feb 28, 2018), 
I see another evolution in the works for structured streaming that does away with offsets and 
also facilitates "continuous streaming".

Static distributed data with a schema is referred to as a relation, while a structured 
datastream with a schema is referred to as a source. 
The schema can be pre-determined, user-provided or inferred from the datasource.
The following references provide more details on RDDs and structure data streams.

[Matei Zahari's Original Spark Paper](http://people.csail.mit.edu/matei/papers/2012/nsdi_spark.pdf)

[Matei Zahari's PhD Thesis on Spark](https://www2.eecs.berkeley.edu/Pubs/TechRpts/2011/EECS-2011-82.pdf)

[Introduction to Structured Streaming (Video) - Part 1](https://databricks.com/session/easy-scalable-fault-tolerant-stream-processing-with-structured-streaming-in-apache-spark)

[Introduction to Structured Streaming (Video) - Part 2](https://databricks.com/session/easy-scalable-fault-tolerant-stream-processing-with-structured-streaming-in-apache-spark-continues)

[RDD Chapter from Jacek Laskowski's Online Git-Book on Spark](https://jaceklaskowski.gitbooks.io/mastering-apache-spark/content/spark-rdd.html)

[Jacek Laskowski's Online Git-Book on Spark Streaming](https://jaceklaskowski.gitbooks.io/spark-structured-streaming/)


The foundation for both batch and streaming datasources is RDD as described in the original paper. 
An RDD (*** link ***) (abstract class) in turn, is composed of zero, one or more partitions (*** link ***) (trait). 
In batch, an RDD and its schema are provided by implementing the abstract class BaseRelation (*** link ***)
(do you see any correlation to a table and relation in relational database theory?!). 
In streaming, the same is achieved by implementing the abstract class Source (*** link ***). 
And finally, you make Spark aware of your batch or streaming datasource by 
implementing DatasourceRegister (*** link ***) and RelationProvider (*** link ***) (batch) or 
StreamSourceProvider (*** link ***) (streaming).

Streaming has an additional notion of the datasource being a continuous, source of data 
wherein each data tuple (row) is uniquely identified by an offset (similar to offset in Kafka)
and the streaming Source is required to have the ability to generate an RDD from a specified start and end offset.


# Sample Datasource Connectors

This project contains very trivial data source connectors that generate a simple string. 
For batch, the generated string is of the form "Partition: N, row X of Y" 
where N is the partition number and for streaming, the string is of the form "Partition: N for time T, row X of Y"


# Build and Test

The project can be build using `sbt assembly` and tested using spark-shell as follows:

## Batch Datasource
```
spark-shell --jars target/scala-2.11/sparkconn-assembly-0.1.jar
....
val mydata = spark.read.format("MyDataProvider").option("numpartitions", "5").option("rowsperpartition", "10").load()
mydata.show(100, false)
```


## Streaming Datasource
```
spark-shell --jars target/scala-2.11/sparkconn-assembly-0.1.jar
....
import org.apache.spark.sql.streaming.Trigger

import org.apache.spark.sql.streaming.ProcessingTime

val mydataStream = spark.readStream.format("MyDataStreamProvider").option("numpartitions", "5").option("rowsperpartition", "10").load()

val query = mydataStream.writeStream.outputMode("append").format("console").option("truncate", "false").option("numRows", "100").trigger(ProcessingTime("5 second")).start()

Thread.sleep(15000)

query.stop()

```