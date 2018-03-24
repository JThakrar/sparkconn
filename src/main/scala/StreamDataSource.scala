
import org.apache.spark.sql.{DataFrame, Dataset, Row, SQLContext}
import org.apache.spark.sql.catalyst.{InternalRow}
import org.apache.spark.sql.execution.streaming.{Offset, Source}
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSourceProvider}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.unsafe.types.UTF8String

package org.apache.spark.sql.jayesh {

  class StreamDataProvider
    extends DataSourceRegister
      with StreamSourceProvider {

    val DEFAULT_NUM_PARTITIONS = "2"
    val DEFAULT_ROWS_PER_PARTITION = "5"

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
      val numPartitions: Int = parameters.getOrElse("numpartitions", DEFAULT_NUM_PARTITIONS).toInt
      val rowsPerPartition: Int = parameters.getOrElse("rowsperpartition", DEFAULT_ROWS_PER_PARTITION).toInt
      new StreamDataSource(sqlContext, schema.getOrElse(myDataStreamSchema), numPartitions, rowsPerPartition)
    }

  }

  class StreamDataSource(sqlContext: SQLContext,
                         override val schema: StructType,
                         numPartitions: Int,
                         numRowsPerPartition: Int)
    extends Source {

    override def getOffset: Option[Offset] = Some(new StreamDataOffset(offset = System.currentTimeMillis()))

    override def commit(end: Offset): Unit = {}

    override def stop: Unit = {}

    override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
      val batchStreamTime = System.currentTimeMillis() // end.asInstanceOf[StreamDataOffset].value
      val rdd: RDD[Row] = new StreamDataRDD(sqlContext.sparkContext, batchStreamTime, numPartitions, numRowsPerPartition)
      val internalRow = rdd.map(row => InternalRow(UTF8String.fromString(row.get(0).asInstanceOf[String])))
      sqlContext.internalCreateDataFrame(internalRow, schema, isStreaming = true)
    }

  }

  class StreamDataOffset(offset: Long)
    extends Offset {

    def value: Long = offset

    override def json: String = s"""{"offset" : ${offset}}"""

  }

  class StreamDataRDD(_sc: SparkContext,
                      batchStreamTime: Long,
                      numPartitions: Int,
                      rowsPerPartition: Int)
    extends RDD[Row](_sc, Nil) {
    override def getPartitions: Array[Partition] = {
      val partitionIds: Seq[Int] = 0 until numPartitions
      val partitions = partitionIds.map(partitionId => new StreamDataPartition(partitionId, batchStreamTime, rowsPerPartition))
      partitions.toArray
    }

    override def compute(partition: Partition, context: TaskContext): Iterator[Row] = {
      val myDataSourcePartition = partition.asInstanceOf[StreamDataPartition]
      val partitionId = myDataSourcePartition.index
      val rows = myDataSourcePartition.rowCount
      val time = myDataSourcePartition.batchStreamTime
      val partitionData = 1 to rows map (r => Row(s"Partition: ${partitionId} for time ${time}, row ${r} of ${rows}"))
      partitionData.iterator
    }

  }


  class StreamDataPartition(partitionId: Int, time: Long, rows: Int)
    extends Partition
      with Serializable {

    override def index: Int = partitionId

    override def toString: String = s"Partition: ${partitionId}  Time: ${time}  Rows: ${rows}"

    def batchStreamTime: Long = time

    def rowCount: Int = rows

  }

}