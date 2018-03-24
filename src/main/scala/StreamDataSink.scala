
import java.io.Serializable
import java.util.Optional

import scala.collection.JavaConverters

import org.apache.spark.sql.{Row, SQLContext, SaveMode}
import org.apache.spark.sql.sources.v2.{DataSourceOptions, StreamWriteSupport}
import org.apache.spark.sql.sources.v2.writer.{DataSourceWriter, DataWriter, DataWriterFactory, WriterCommitMessage}
import org.apache.spark.sql.sources.v2.writer.streaming.StreamWriter
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType


class StreamDataSink
  extends StreamWriteSupport {

  override def createStreamWriter(queryId: java.lang.String,
                                  schema: StructType,
                                  mode: OutputMode,
                                  options: DataSourceOptions): StreamWriter = {

    new StreamDataSinkWriter(queryId, schema, mode, options)
  }

}


class StreamDataSinkWriter(queryId: java.lang.String,
                           schema: StructType,
                           mode: OutputMode,
                           options: DataSourceOptions)
extends StreamWriter {

  override def createWriterFactory(): DataWriterFactory[Row] = {
    new StreamDataWriterFactory[Row](queryId, schema, mode)
  }

  override def commit(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {
    println(s"Commiting: queryId = ${queryId}, epochId = ${epochId}, mode = ${mode}")
    messages.foreach(println)
  }

  override def abort(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {
    println(s"Commiting: queryId = ${queryId}, epochId = ${epochId}, mode = ${mode}")
    messages.foreach(println)
  }

}

class StreamDataWriterFactory[Row](queryId: java.lang.String,
                                   schema: StructType,
                                   mode: OutputMode)
  extends DataWriterFactory[Row] with Serializable {

  override def createDataWriter(partitionId: Int,  attemptNumber: Int): DataWriter[Row] = {
    (new StreamDataWriter(queryId, schema, mode, partitionId, attemptNumber)).asInstanceOf[DataWriter[Row]]
  }
}


class StreamDataWriter(queryId: java.lang.String,
                       schema: StructType,
                       mode: OutputMode,
                       partitionId: Int,
                       attemptNumber: Int)
  extends DataWriter[Row]{

  override def write(row: Row): Unit =
  // Note that currently, the line below is printed/output for EACH row. That is the streaming sink.
  // Also note that ideally the writer/sink does/may not know the schema apriori,
  // and so the write logic needs to extract the individual fields from the provided schema.
    println(s"JobId: ${queryId} | Partition: ${partitionId}, AttemptNumber: ${attemptNumber} | *** ${row} ***")

  override def commit: WriterCommitMessage = StreamDataCommitMessage(queryId, partitionId, attemptNumber)

  override def abort(): Unit =
    println(s"JobId: ${queryId} | Partition: ${partitionId}, AttemptNumber: ${attemptNumber} | *** >>> ABORTED <<< ***")

}


case class StreamDataCommitMessage(queryId: java.lang.String, partitionId: Int, attemptNumber: Int) extends WriterCommitMessage
