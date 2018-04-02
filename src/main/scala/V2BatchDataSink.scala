
import java.io.Serializable
import java.util.Optional

import org.apache.spark.sql.{Row, SQLContext, SaveMode}
import org.apache.spark.sql.sources.v2.{DataSourceOptions, WriteSupport}
import org.apache.spark.sql.sources.v2.writer.{DataSourceWriter, DataWriter, DataWriterFactory, WriterCommitMessage}
import org.apache.spark.sql.types.StructType

class V2BatchDataSink
  extends WriteSupport {

  override def createWriter(jobId: java.lang.String,
                            schema: StructType,
                            mode: SaveMode,
                            options: DataSourceOptions): Optional[DataSourceWriter] = {
    // Note that found that "options" is not seriablizable - so if you want to "pass down" any information
    // contained in it, you need to extract it, ensure that is serializable and then pass it downstream.
    Optional.of(new V2BatchDataSinkWriter(jobId, schema, mode))
  }
}


class V2BatchDataSinkWriter(jobId: java.lang.String,
                            schema: StructType,
                            mode: SaveMode)
  extends DataSourceWriter {

  override def createWriterFactory(): DataWriterFactory[Row] = {
    println(s"Creating data sink for schema:\n${schema}\n")
    new V2BatchDataWriterFactory(jobId, schema, mode)
  }

  override def commit(messages: Array[WriterCommitMessage]): Unit = {
    println(s"Commiting: JobId = ${jobId}, mode = ${mode}")
    messages.foreach(println)
  }

  override def abort(messages: Array[WriterCommitMessage]): Unit = {
    println(s"Aborting: JobId = ${jobId}, mode = ${mode}")
    messages.foreach(println)
  }
}


class V2BatchDataWriterFactory[Row](jobId: java.lang.String,
                                    schema: StructType,
                                    mode: SaveMode)
  extends DataWriterFactory[Row] with Serializable {

  override def createDataWriter(partitionId: Int,
                                attemptNumber: Int): DataWriter[Row] = {
    (new V2BatchDataWriter(jobId, schema, mode, partitionId, attemptNumber)).asInstanceOf[DataWriter[Row]]
  }
}


class V2BatchDataWriter(jobId: java.lang.String,
                        schema: StructType,
                        mode: SaveMode,
                        partitionId: Int,
                        attemptNumber: Int)
  extends DataWriter[Row]{

  override def write(row: Row): Unit =
    // Note that currently, the line below is printed/output for EACH row. That is the batch sink.
    // Also note that ideally the writer/sink does/may not know the schema apriori,
    // and so the write logic needs to extract the individual fields from the provided schema.
    println(s"JobId: ${jobId} | Partition: ${partitionId}, AttemptNumber: ${attemptNumber} | *** ${row} ***")

  override def commit: WriterCommitMessage = V2BatchDataCommitMessage(jobId, partitionId, attemptNumber)

  override def abort(): Unit =
    println(s"JobId: ${jobId} | Partition: ${partitionId}, AttemptNumber: ${attemptNumber} | *** >>> ABORTED <<< ***")

}


case class V2BatchDataCommitMessage(jobId: java.lang.String,
                                    partitionId: Int,
                                    attemptNumber: Int)
  extends WriterCommitMessage
