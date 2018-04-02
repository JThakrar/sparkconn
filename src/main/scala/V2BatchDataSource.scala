
import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.v2.reader.{DataReader, DataReaderFactory, DataSourceReader}
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.collection.JavaConverters._

/**
  * Batch (non-streaming) data source using the V2 API.
  * Example Usage:
  *{{{
  *
  * scala> val data = spark.read.format("V2BatchDataSource")
  * scala> data.show(50, false)
  *
  *}}}
  */
class V2BatchDataSource
  extends DataSourceRegister with DataSourceV2 with ReadSupport {

  val DEFAULT_PARTITION_COUNT: Int = 5

  val DEFAULT_ROWS_PER_PARTITION: Int = 5

  override def shortName(): String = "v2batchdatasource"

  override def createReader (options: DataSourceOptions): DataSourceReader = {
    val optionsKV = options.asMap().asScala // converts options to lower-cased keyed map
    val partitions = optionsKV.getOrElse("partitions", DEFAULT_PARTITION_COUNT).toString.toInt
    val rows = optionsKV.getOrElse("rowsPerPartition", DEFAULT_ROWS_PER_PARTITION).toString.toInt
    assert(partitions > 0, s"Partitions should be > 0 (specified value = ${partitions})")
    assert (rows > 0, s"Rows should be > 0 (specified value = ${rows})")
    println(s"\n\nCreating ${this.getClass.getName}: with ${partitions} partitions, each with ${rows} rowsPerPartition\n")
    new V2BatchDataSourceReader(partitions, rows)
  }
}


class V2BatchDataSourceReader(partitions: Int,
                              rows: Int)
  extends DataSourceReader {

  println(s"\n\nCreating ${this.getClass.getName}: ${partitions} partitions and ${rows} rowsPerPartition each\n")

  override def readSchema(): StructType = {
    StructType(StructField("string_value", StringType, false)::Nil)
  }

  override def createDataReaderFactories(): java.util.List[DataReaderFactory[Row]] = {
    println("Creating DataReaderFactories.........")
    (1 to partitions).map(partition =>
      new V2BatchDataReaderFactory(partition, rows, partitions).asInstanceOf[DataReaderFactory[Row]]).asJava
  }
}


class V2BatchDataReaderFactory(partitionNumber: Int,
                               rows: Int,
                               totalPartitions: Int)
  extends DataReaderFactory[Row] {

  println(s"\n\nCreating ${this.getClass.getName}: ${partitionNumber} of ${totalPartitions}\n")

  override def createDataReader(): DataReader[Row] = {
    new V2BatchDataReader(partitionNumber, rows)
  }

}


class V2BatchDataReader(partitionNumber: Int,
                        rows: Int)
  extends DataReader[Row] {

  private var rowsRemaining = rows

  println(s"\n\nCreating ${this.getClass.getName}: ${partitionNumber}\n")

  override def next(): Boolean = rowsRemaining > 0

  override def get(): Row = {
    var resultRow: Row = Row("")

    if (next) {
      rowsRemaining = rowsRemaining - 1
      resultRow = Row(s"Partition: ${partitionNumber} || Row ${rows - rowsRemaining} of ${rows}")
    } else {
      new IllegalArgumentException
    }
    resultRow
  }

  override def close(): Unit = {}

}
