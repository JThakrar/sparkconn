
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD


// https://docs.databricks.com/spark/latest/rdd-streaming/tips-for-running-streaming-apps-in-databricks.html

/**
  * MydataProvider class is referenced to the format method when reading data from a source.
  * E.g. spark.read.format("MyDataProvider")
  */
class BatchDataProvider
  extends DataSourceRegister
  with RelationProvider
  with SchemaRelationProvider {

  val DEFAULT_NUM_PARTITIONS = "2"
  val DEFAULT_ROWS_PER_PARTITION = "5"

  override def shortName():String = "mydata"

  private val myDataSchema: StructType = new StructType(Array[StructField](new StructField("string", StringType, false)))

  // Required for "RelationProvider" trait. The schema is assumed to be either fixed or inferrable.
  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String]): BaseRelation = {
    val numPartitions: Int = parameters.getOrElse("numpartitions", DEFAULT_NUM_PARTITIONS).toInt
    val rowsPerPartition: Int = parameters.getOrElse("rowsperpartition", DEFAULT_ROWS_PER_PARTITION).toInt
    new BatchDataRelation(sqlContext, myDataSchema, numPartitions, rowsPerPartition)
  }

  // Required for SchemaRelationProvider trait.
  // Note that for the purpose of this example, we are ignoring the userProvidedSchema
  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String],
                              userProvidedSchema: StructType): BaseRelation = {
    val numPartitions: Int = parameters.getOrElse("numpartitions", DEFAULT_NUM_PARTITIONS).toInt
    val rowsPerPartition: Int = parameters.getOrElse("rowsperpartition", DEFAULT_ROWS_PER_PARTITION).toInt
    new BatchDataRelation(sqlContext, myDataSchema, numPartitions, rowsPerPartition)
  }

}


class BatchDataRelation(override val sqlContext: SQLContext,
                        override val schema: StructType,
                        numPartitions: Int,
                        rowsPerPartition: Int)
  extends BaseRelation
  with TableScan {

  override def buildScan(): RDD[Row] = {
    val rdd = new BatchDataRDD(sqlContext.sparkContext, numPartitions, rowsPerPartition)
    rdd
  }

  override def needConversion = true

}


class BatchDataRDD(sc: SparkContext,
                   numPartitions: Int,
                   rowsPerPartition: Int)
  extends RDD[Row](sc, Nil) {

  override def getPartitions: Array[Partition] = {
    val partitionSeq: Seq[Int] = 0 until numPartitions
    val partitions: Seq[Partition] =  partitionSeq.map(partitionId => new BatchDataPartition(partitionId,rowsPerPartition))
    partitions.toArray
  }

  override def compute(partition: Partition, context: TaskContext): Iterator[Row] = {
    val myDataPartition = partition.asInstanceOf[BatchDataPartition]
    val partitionId = myDataPartition.index
    val rows = myDataPartition.rowCount
    val partitionData = 1 to rows map(r => Row(s"Partition: ${partitionId}, row ${r} of ${rows}"))
    partitionData.iterator
  }

}


class BatchDataPartition(partitionId: Int, rows: Int)
  extends Partition
  with Serializable {

  override def index: Int = partitionId

  override def toString: String = s"Partition: ${partitionId}  Rows: ${rows}"

  def rowCount: Int = rows

}
