
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.execution.streaming.{Offset, Source}
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSourceProvider}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD

/**
  * import org.apache.spark.sql.streaming.Trigger
  * import org.apache.spark.sql.streaming.ProcessingTime
  *
  * val mydataStream = spark.readStream.
  * format("MyDataStreamProvider").
  * option("spark.mydatastream.numpartitions", "5").
  * option("spark.mydatastream.rowsperpartition", "20").
  * load()
  *
  * val query = mydataStream.
  * writeStream.
  * outputMode("append").
  * format("console").
  * trigger(ProcessingTime("5 second")).
  * option("truncate", "false").
  * start()
  *
  * Thread.sleep(15000)
  * query.stop()
  */

class MyDataStreamProvider extends DataSourceRegister with StreamSourceProvider with Logging {

  private val myDataStreamSchema: StructType = new StructType(Array[StructField](new StructField("string", StringType, false)))

  override def shortName(): String = "mydatastream"

  override def sourceSchema(sqlContext: SQLContext,
                            schema: Option[StructType],
                            providerName: String,
                            parameters: Map[String, String]): (String, StructType) = {
    ("MyDataStream", schema.getOrElse(myDataStreamSchema))
  }

  override def createSource(sqlContext: SQLContext,
                            metadataPath: String,
                            schema: Option[StructType],
                            providerName: String, parameters: Map[String, String]): Source = {
    val numPartitions: Int = parameters.getOrElse("spark.mydatastream.numpartitions", "1").toInt
    val rowsPerPartition: Int = parameters.getOrElse("spark.mydatastream.rowsperpartition", "5").toInt
    new MyDataStreamSource(sqlContext, schema.getOrElse(myDataStreamSchema), numPartitions, rowsPerPartition)
  }

}

class MyDataStreamSource(sqlContext: SQLContext,
                         override val schema: StructType,
                         numPartitions: Int,
                         numRowsPerPartition: Int) extends Source {

  override def getOffset: Option[Offset] = Some(new MyDataStreamOffset(offset = System.currentTimeMillis()))

  override def commit(end: Offset): Unit = {}

  override def stop: Unit = {}

  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    val streamTime = end.asInstanceOf[MyDataStreamOffset].value
    val rdd: RDD[Row] = new MyDataStreamRDD(sqlContext.sparkContext, streamTime, numPartitions, numRowsPerPartition)
    sqlContext.createDataFrame(rdd, schema)
  }

}

class MyDataStreamOffset(offset: Long) extends Offset {

  def value: Long = offset

  override def json: String = s"""{"offset" : ${offset}}"""

}

class MyDataStreamRDD(_sc: SparkContext,
                      streamTime: Long,
                      numPartitions: Int,
                      rowsPerPartition: Int) extends RDD[Row](_sc, Nil) {

  override def getPartitions: Array[Partition] = {
    val partitions = 0 until numPartitions map(partitionId => new MyDataStreamPartition(partitionId, streamTime, rowsPerPartition))
    partitions.toArray
  }

  override def compute(partition: Partition, context: TaskContext): Iterator[Row] = {
    val myDataSourcePartition = partition.asInstanceOf[MyDataStreamPartition]
    val partitionId = myDataSourcePartition.index
    val rows = myDataSourcePartition.rowCount
    val time = myDataSourcePartition.streamTime
    val partitionData = 1 to rows map(r => Row(s"Partition: ${partitionId} for time ${time}, row ${r} of ${rows}"))
    partitionData.iterator
  }

}


class MyDataStreamPartition(partitionId: Int, time: Long, rows: Int) extends Partition with Serializable {

  override def index: Int = partitionId

  override def toString: String = s"Partition: ${partitionId}  Time: ${time}  Rows: ${rows}"

  def streamTime: Long = time

  def rowCount: Int = rows

}
