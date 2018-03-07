
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister, RelationProvider, TableScan}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD


// https://docs.databricks.com/spark/latest/rdd-streaming/tips-for-running-streaming-apps-in-databricks.html

/**
  * spark-shell --jars target/scala-2.11/sparkconn-assembly-0.1.jar
  *
  * val mydata = spark.read.format("MyDataProvider").option("numpartitions", "5").option("rowsperpartition", "10").load()
  * mydata.show(100, false)
  *
  */

class MyDataProvider extends DataSourceRegister with RelationProvider with Logging {

  override def shortName():String = { "mydata" }

  private val myDataSchema: StructType = new StructType(Array[StructField](new StructField("string", StringType, false)))

  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String]): BaseRelation = {
    val numPartitions: Int = parameters.getOrElse("numpartitions", "2").toInt
    val rowsPerPartition: Int = parameters.getOrElse("rowsperpartition", "5").toInt
    new MyDataRelation(sqlContext, myDataSchema, numPartitions, rowsPerPartition)
  }

}


class MyDataRelation(override val sqlContext: SQLContext,
                     override val schema: StructType,
                     numPartitions: Int,
                     rowsPerPartition: Int) extends BaseRelation with TableScan with Logging {

  override def buildScan(): RDD[Row] = {
    val rdd = new MyDataRDD(sqlContext.sparkContext, numPartitions, rowsPerPartition)
    rdd
  }

  override def needConversion = true

}


class MyDataRDD(sc: SparkContext,
                numPartitions: Int,
                rowsPerPartition: Int) extends RDD[Row](sc, Nil) {

  override def getPartitions: Array[Partition] = {
    val partitionSeq: Seq[Int] = 0 until numPartitions
    val partitions: Seq[Partition] =  partitionSeq.map(partitionId => new MyDataPartition(partitionId,rowsPerPartition))
    partitions.toArray
  }

  override def compute(partition: Partition, context: TaskContext): Iterator[Row] = {
    val myDataPartition = partition.asInstanceOf[MyDataPartition]
    val partitionId = myDataPartition.index
    val rows = myDataPartition.rowCount
    val partitionData = 1 to rows map(r => Row(s"Partition: ${partitionId}, row ${r} of ${rows}"))
    partitionData.iterator
  }

}


class MyDataPartition(partitionId: Int, rows: Int) extends Partition with Serializable {

  override def index: Int = partitionId

  override def toString: String = s"Partition: ${partitionId}  Rows: ${rows}"

  def rowCount: Int = rows

}
