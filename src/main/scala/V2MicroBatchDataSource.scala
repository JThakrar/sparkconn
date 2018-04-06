

import java.util.Optional

import scala.collection.JavaConverters._

import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.v2.reader.{DataReader, DataReaderFactory}
import org.apache.spark.sql.sources.v2.reader.streaming.{MicroBatchReader, Offset}
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, MicroBatchReadSupport}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization

class V2MicroBatchDataSource
  extends DataSourceRegister
    with DataSourceV2
    with MicroBatchReadSupport {

  println(s"\n\nCreating ${this.getClass.getName}.....\n")

  val DEFAULT_PARTITION_COUNT: Int = 5

  val DEFAULT_ROWS_PER_PARTITION_PER_BATCH: Int = 5

  override def shortName(): String = "v2microbatchdatasource"

  override def createMicroBatchReader(schema: Optional[StructType],
                                      checkpointLocation: String,
                                      options: DataSourceOptions): MicroBatchReader = {
    val optionsKV = options.asMap().asScala // converts options to lower-cased keyed map
    val partitions = optionsKV.getOrElse("partitions", DEFAULT_PARTITION_COUNT).toString.toInt
    val rows = optionsKV.getOrElse("rowsperpartition", DEFAULT_ROWS_PER_PARTITION_PER_BATCH).toString.toInt
    assert(partitions > 0, s"Partitions should be > 0 (specified value = ${partitions})")
    assert (rows > 0, s"Rows should be > 0 (specified value = ${rows})")
    new V2MicroBatchDataSourceReader(partitions, rows)
  }

}


class V2MicroBatchDataSourceReader(partitions: Int,
                                   rowsPerPartition: Int)
  extends MicroBatchReader {

  println(s"\n\nCreating ${this.getClass.getName}: ${partitions} partitions, ${rowsPerPartition} rows/partition\n")

  private var startOffset: V2MicroBatchOffset = V2MicroBatchOffset(0)
  private var endOffset: V2MicroBatchOffset = V2MicroBatchOffset(0 + rowsPerPartition)

  println(s"\n\nInitializing: startOffset = ${startOffset}, endOffset = ${endOffset}\n")

  override def stop(): Unit = {
    println(s"\n\nStopping ${this.getClass.getName} with start offset = ${startOffset} and end offset = ${endOffset}\n")
  }

  override def readSchema: StructType = {
    println(s"\n\n${this.getClass.getName}.readSchema called\n")
    StructType(StructField("string_value", StringType, false)::Nil)
  }

  override def createDataReaderFactories: java.util.List[DataReaderFactory[Row]] = {
    println(s"\n\n${this.getClass.getName}.createDataReaderFactories called\n")
    (1 to partitions).map(p =>
      new V2MicroBatchDataReaderFactory(p, startOffset, endOffset).asInstanceOf[DataReaderFactory[Row]]).asJava
  }

  override def setOffsetRange(start: Optional[Offset], end: Optional[Offset]): Unit = {
    println(s"\n\n${this.getClass.getName}.setOffsetRange called\n")
    val s: String = if (start.isPresent) start.toString else "*** null ***"
    val e: String = if (end.isPresent) end.toString else "*** null ***"

    println(s"\n\nsetOffsetRange called with following offsets: Start = ${s}, End = ${e}\n")

    if (start.isPresent & !end.isPresent) {
      val offset = start.get.asInstanceOf[V2MicroBatchOffset]
      startOffset = offset
      endOffset = V2MicroBatchOffset(offset.rowNum + rowsPerPartition)
      println(s"\n\nAfter setting start: Start = ${startOffset}, End = ${endOffset}\n")
    }

    if (end.isPresent & !end.isPresent) {
      val offset = end.get.asInstanceOf[V2MicroBatchOffset]
      endOffset = offset
      startOffset = V2MicroBatchOffset(offset.rowNum - rowsPerPartition)
      println(s"\n\nAfter setting end: Start = ${startOffset}, End = ${endOffset}\n")
    }

    if (start.isPresent & end.isPresent) {
      startOffset = start.get.asInstanceOf[V2MicroBatchOffset]
      endOffset = end.get.asInstanceOf[V2MicroBatchOffset]
    }

  }

  override def getStartOffset: Offset = {
    println(s"\n\n${this.getClass.getName}.getStartOffset called (start offset = ${startOffset})\n")
    startOffset
  }

  override def getEndOffset: Offset = {
    println(s"\n\n${this.getClass.getName}.getEndOffset called (end offset = ${endOffset})\n")
    endOffset
  }

  override def commit(offset: Offset): Unit = {
    println(s"\n\n${this.getClass.getName}.commit(${offset}) called\n")
    val v2MicroBatchOffset = offset.asInstanceOf[V2MicroBatchOffset]
    endOffset = offset.asInstanceOf[V2MicroBatchOffset]
  }

  override def deserializeOffset(json: String): Offset = {
    implicit val formats = Serialization.formats(NoTypeHints)
    val offset = Serialization.read[V2MicroBatchOffset](json)
    offset.asInstanceOf[Offset]
  }

}


class V2MicroBatchDataReaderFactory(partition: Int,
                                    startOffset: V2MicroBatchOffset,
                                    endOffset: V2MicroBatchOffset)
  extends DataReaderFactory[Row] {

  println(s"\n\nCreating ${this.getClass.getName} (partitions = ${partition}, startOffset = ${startOffset}, endOffset = ${endOffset}")

  override def createDataReader: DataReader[Row] = {
    new V2MicroBatchDataReader(partition, startOffset, endOffset)
  }

}


class V2MicroBatchDataReader(partitionNumber: Int,
                             startOffset: V2MicroBatchOffset,
                             endOffset: V2MicroBatchOffset)
  extends DataReader[Row] {

  println(s"\n\nCreating ${this.getClass.getName} (partition = ${partitionNumber}: startOffset = ${startOffset}, endOffset = ${endOffset})\n")

  var startRow = startOffset.rowNum
  val endRow = endOffset.rowNum

  override def next(): Boolean = endRow > startRow

  override def get(): Row = {
    var resultRow: Row = Row("<empty>")

    if (next) {
      startRow += 1
      resultRow = Row(s"Partition: ${partitionNumber} || Row ${startRow} in range of (${startOffset} through ${endOffset}]")
    } else {
      new IllegalArgumentException
    }
    resultRow
  }

  override def close(): Unit = {}

}


case class V2MicroBatchOffset(rowNum: Int)
  extends Offset {

  override def json: String = {
    implicit val formats = Serialization.formats(NoTypeHints)
    Serialization.write(this)
  }

}
