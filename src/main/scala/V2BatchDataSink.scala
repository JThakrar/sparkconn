
import java.io.Serializable
import java.util.Optional

import org.apache.spark.sql.{Row, SQLContext, SaveMode}
import org.apache.spark.sql.sources.v2.{DataSourceOptions, WriteSupport}
import org.apache.spark.sql.sources.v2.writer.{DataSourceWriter, DataWriter, DataWriterFactory, WriterCommitMessage}
import org.apache.spark.sql.types.StructType

/**
  * Batch (non-streaming) data sink using V2 API
  */
class V2BatchDataSink
  extends WriteSupport {

  /**
    * Found that "options" is not seriablizable - so if you want to pass any information
    * downstream, need to extract and make it serializable.
    * @param jobId
    * @param schema
    * @param mode
    * @param options
    * @return
    */
  override def createWriter(jobId: java.lang.String,
                            schema: StructType,
                            mode: SaveMode,
                            options: DataSourceOptions): Optional[DataSourceWriter] = {
    Optional.of(new V2BatchDataSinkWriter(jobId, schema, mode))
  }
}

/**
  * Note that unlike a DataSourceReader, the DataSourceWriter does not need to determine partitions
  * as there is one-to-one correspondence between the reader and writer.
  * The writer gets schema and the savemode and can use it accordingly.
  * @param jobId
  * @param schema
  * @param mode
  */
class V2BatchDataSinkWriter(jobId: java.lang.String,
                            schema: StructType,
                            mode: SaveMode)
  extends DataSourceWriter {

  override def createWriterFactory(): DataWriterFactory[Row] = {
    println(s"Creating data sink for schema:\n${schema}\n")
    new V2BatchDataWriterFactory(jobId, schema, mode)
  }

  override def commit(messages: Array[WriterCommitMessage]): Unit = {
    println(s"Commiting: JobId = ${jobId}, mode = ${mode} with the following ${messages.length} commit messages")
    messages.foreach(println)
  }

  override def abort(messages: Array[WriterCommitMessage]): Unit = {
    println(s"Aborting: JobId = ${jobId}, mode = ${mode} with the following ${messages.length} commit messages")
    messages.foreach(println)
  }
}

/**
  * This is simply a handle to instantiate the actual writer which also handles the commit messages.
  * @param jobId
  * @param schema
  * @param mode
  * @tparam Row
  */
class V2BatchDataWriterFactory[Row](jobId: java.lang.String,
                                    schema: StructType,
                                    mode: SaveMode)
  extends DataWriterFactory[Row] with Serializable {

  override def createDataWriter(partitionId: Int,
                                attemptNumber: Int): DataWriter[Row] = {
    (new V2BatchDataWriter(jobId, schema, mode, partitionId, attemptNumber)).asInstanceOf[DataWriter[Row]]
  }
}

/**
  * This is where the actual work of writing and committing the data gets done.
  * @param jobId
  * @param schema
  * @param mode
  * @param partitionId
  * @param attemptNumber
  */
class V2BatchDataWriter(jobId: java.lang.String,
                        schema: StructType,
                        mode: SaveMode,
                        partitionId: Int,
                        attemptNumber: Int)
  extends DataWriter[Row]{

  /**
    * Note that in this implementation, write is simply printing the row.
    * Also note that ideally the writer/sink does/may not know the schema apriori,
    * and so the write logic may need to extract the individual fields from
    * the row ({@link org.apache.spark.sql.Row}) using the provided
    * schema ({@link org.apache.spark.sql.types.StructType}
    *
    * @param row
    */
  override def write(row: Row): Unit =
    println(s"JobId: ${jobId} | Partition: ${partitionId}, AttemptNumber: ${attemptNumber} | *** ${row} ***")

  override def commit: WriterCommitMessage = V2BatchDataCommitMessage(jobId, partitionId, attemptNumber)

  override def abort(): Unit =
    println(s"JobId: ${jobId} | Partition: ${partitionId}, AttemptNumber: ${attemptNumber} | *** >>> ABORTED <<< ***")

}


case class V2BatchDataCommitMessage(jobId: java.lang.String,
                                    partitionId: Int,
                                    attemptNumber: Int)
  extends WriterCommitMessage
