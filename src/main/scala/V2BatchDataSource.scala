
import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.v2.reader.{DataReader, DataReaderFactory, DataSourceReader}
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport}
import org.apache.spark.sql.types.{StringType, StructField, StructType}


import scala.collection.JavaConverters._


/**
  * Batch (non-streaming) data source using the V2 API.
  * V2BatchDataSource class name is the entry point that needs to be specified to a dataframe reader.
  * This then enables instantiating a DataSourceReader.
  * Example Usage:
  * {{{
  *
  * scala> val data = spark.read.format("V2BatchDataSource").load()
  * scala> data.show(25, false)
  *
  * }}}
  */
class V2BatchDataSource
  extends DataSourceRegister
    with ReadSupport { // use ReadSupportWithSchema to support user-provided Schema

  val DEFAULT_PARTITION_COUNT: Int = 5

  val DEFAULT_ROWS_PER_PARTITION: Int = 5

  override def shortName(): String = "v2batchdatasource" // to implement DataSourceRegister

  override def createReader(options: DataSourceOptions): DataSourceReader = { // to implement ReadSupport
    val optionsKV = options.asMap().asScala // converts options to lower-cased keyed map
    val partitions = optionsKV.getOrElse("partitions", DEFAULT_PARTITION_COUNT).toString.toInt
    val rows = optionsKV.getOrElse("rowsperpartition", DEFAULT_ROWS_PER_PARTITION).toString.toInt
    assert(partitions > 0, s"Partitions should be > 0 (specified value = $partitions)")
    assert(rows > 0, s"Rows should be > 0 (specified value = $rows)")
    println(s"\n\nCreating ${this.getClass.getName}: with $partitions partitions, each with $rows rowsPerPartition\n")
    new V2BatchDataSourceReader(partitions, rows)
  }
}


/**
  * V2BatchDataSourceReader implements DataSourceReader.
  * It needs to know the schema of the source data - which can be pre-determined,
  * user-supplied or to be determined at run-time.
  * Furthermore, it should also know the number of data partitions for the source
  * as it will need to create that many DataReaderFactory objects.
  *
  * @param partitions
  * @param rows
  */
class V2BatchDataSourceReader(partitions: Int,
                              rows: Int)
  extends DataSourceReader {

  println(s"\n\nCreating ${this.getClass.getName}: $partitions partitions and $rows rowsPerPartition each\n")

  override def readSchema(): StructType = {
    StructType(StructField("string_value", StringType, false) :: Nil)
  }

  override def createDataReaderFactories(): java.util.List[DataReaderFactory[Row]] = {
    println("Creating DataReaderFactories.........")
    (1 to partitions).map(partition =>
      new V2BatchDataReaderFactory(partition, rows, partitions).asInstanceOf[DataReaderFactory[Row]]).asJava
  }
}


/**
  * As the documentation for {@link org.apache.spark.sql.sources.v2.reader.DataReaderFactory}
  * indicates, this class is like an iterable (an object that provides an iterator)
  * and provides the corresponding DataReader which is the iterator equivalent.
  * Note that the DataReaderFactory is instantiated at the Driver and then serialized
  * and sent to each executor where DataReader is later deserialized
  * to read the actual source data.
  *
  * @param partition
  * @param rows
  * @param totalPartitions
  */
class V2BatchDataReaderFactory(partition: Int,
                               rows: Int,
                               totalPartitions: Int)
  extends DataReaderFactory[Row] {

  println(s"\n\nCreating ${this.getClass.getName}: $partition of $totalPartitions\n")

  override def createDataReader(): DataReader[Row] = {
    new V2BatchDataReader(partition, rows)
  }

}


/**
  * The workhorse logic - read data from the source on behalf of a specific partition.
  *
  * @param partition
  * @param rows
  */
class V2BatchDataReader(partition: Int,
                        rows: Int)
  extends DataReader[Row] {

  private var rowsRemaining = rows

  println(s"\n\nCreating ${this.getClass.getName}: $partition\n")

  override def next(): Boolean = rowsRemaining > 0

  override def get(): Row = {
    var resultRow: Row = Row("")

    if (next) {
      rowsRemaining = rowsRemaining - 1
      resultRow = Row(s"Partition: $partition || Row ${rows - rowsRemaining} of $rows")
    } else {
      new IllegalArgumentException
    }
    resultRow
  }

  override def close(): Unit = {}
}
