
import java.util.Optional

import scala.collection.JavaConverters._
import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.v2.{DataSourceV2, DataSourceOptions, ContinuousReadSupport}
import org.apache.spark.sql.sources.v2.reader.{DataReader, DataReaderFactory}
import org.apache.spark.sql.sources.v2.reader.streaming.{ContinuousDataReader, ContinuousReader}
import org.apache.spark.sql.sources.v2.reader.streaming.PartitionOffset
import org.apache.spark.sql.sources.v2.reader.streaming.Offset
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization

class V2ContinuousDataSource
  extends DataSourceRegister
    with DataSourceV2
    with ContinuousReadSupport {

  println(s"\n\nCreating ${this.getClass.getName}.....\n")

  override def shortName: String = "v2continuousdatasource"

  private val DEFAULT_PARTITION_COUNT = 5

  private val DEFAULT_START_OFFSET = 0

  private val DEFAULT_END_OFFSET = 1000000

  override def createContinuousReader (schema: Optional[StructType],
                                       checkpointLocation: String,
                                       options: DataSourceOptions): ContinuousReader = {
    val optionsKV = options.asMap().asScala // converts options to lower-cased keyed map
    val partitions = optionsKV.getOrElse("partitions", DEFAULT_PARTITION_COUNT.toString).toInt
    val start = optionsKV.getOrElse("start", DEFAULT_START_OFFSET.toString).toInt
    val end = optionsKV.getOrElse("end", DEFAULT_END_OFFSET).toString.toInt
    assert(partitions > 0, s"Partitions should be > 0 (specified value = ${partitions})")
    assert(end > start, s"End offset (${end}) is not greater than start offset (${start})")
    new V2ContinuousDataSourceReader(partitions, start, end)
  }
}

class V2ContinuousDataSourceReader(partitions: Int, start: Int, end: Int)
  extends ContinuousReader {

  println(s"\n\nCreating ${this.getClass.getName} with ${partitions} partitions and start = ${start}, end = ${end}\n")

  private var startOffset: V2ContinuousOffset = V2ContinuousOffset((1 to partitions).map(p => V2ContinuousPartitionOffset(p, start)).toList)
  private var endOffset: V2ContinuousOffset = V2ContinuousOffset((1 to partitions).map(p => V2ContinuousPartitionOffset(p, end)).toList)

  override def mergeOffsets(offsets: Array[PartitionOffset]): Offset = {

    println(s"\n\nMerging offsets \n${offsets.mkString("\n")}\n")

    val v2ContinuousPartitionOffsets: Array[V2ContinuousPartitionOffset] = offsets.map(o => o.asInstanceOf[V2ContinuousPartitionOffset])
    V2ContinuousOffset(v2ContinuousPartitionOffsets.toList)
  }

  override def deserializeOffset (json: String): Offset = {

    println(s"\n\nDeserializing offset ${json}\n")

    implicit val formats = Serialization.formats(NoTypeHints)
    val v2ContinuousOffset = Serialization.read[V2ContinuousOffset](json)
    v2ContinuousOffset.asInstanceOf[Offset]
  }

  override def setStartOffset(start: Optional[Offset]): Unit = {
    if (start.isPresent) {

      println(s"\n\nSetting start offset to ${start.get.json()}\n")

      val offset = start.get.asInstanceOf[V2ContinuousOffset]
      startOffset = offset
    }
  }

  override def getStartOffset: Offset = {
    startOffset.asInstanceOf[Offset]
  }

  override def commit(end: Offset): Unit = {
    println(s"\n\nCommitting end offset to ${end}\n")
  }

  override def readSchema: StructType = {
    println(s"\n\n${this.getClass.getName}.readSchema called\n")
    StructType(StructField("string_value", StringType, false)::Nil)
  }

  override def stop: Unit = {
    println(s"\n\nStopping ${this.getClass.getName} !!!\n")
  }

  override def createDataReaderFactories: java.util.List[DataReaderFactory[Row]] = {
    println(s"\n\n${this.getClass.getName}.createDataReaderFactories() called\n")
    (1 to partitions).map(p =>
      new V2ContinuousDataReaderFactory(p, startOffset.partitionOffsets(p - 1), endOffset.partitionOffsets(p - 1)).asInstanceOf[DataReaderFactory[Row]]).asJava
  }

}

class V2ContinuousDataReaderFactory(partition: Int,
                                    start: V2ContinuousPartitionOffset,
                                    end: V2ContinuousPartitionOffset)
  extends DataReaderFactory[Row] {

  override def createDataReader: DataReader[Row] = {
    new V2ContinuousDataReader(partition, start, end)

  }

}

class V2ContinuousDataReader(partition: Int,
                             start: V2ContinuousPartitionOffset,
                             end: V2ContinuousPartitionOffset)
  extends ContinuousDataReader[Row] {

  private var current: Int = start.offset

  override def next: Boolean = start.offset < end.offset

  override def get: Row = {
    var row: Row = Row("<Empty result>")
    if (next) {
      Thread.sleep(500)
      current += 1
      row = Row(s"Partition: ${partition} || Row ${current} in range of ${start.offset} and ${end.offset}")
    } else {
      new IllegalArgumentException(s"Reached the last row value ${end.offset}")
    }
    row
  }

  override def getOffset: PartitionOffset = {
    V2ContinuousPartitionOffset(partition, current).asInstanceOf[PartitionOffset]
  }

  override def close: Unit = {
    println(s"\n\n${this.getClass.getName}.close() called\n")
  }

}

case class V2ContinuousPartitionOffset(partition: Int, offset: Int)
  extends PartitionOffset {

}

case class V2ContinuousOffset(partitionOffsets: List[V2ContinuousPartitionOffset])
  extends Offset {

  override def json: String = {
    implicit val formats = Serialization.formats(NoTypeHints)
    Serialization.write(this)
  }

}