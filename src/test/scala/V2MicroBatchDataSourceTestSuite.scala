
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.scalatest.FunSuite

class V2MicroBatchDataSourceTestSuite extends FunSuite {

  val spark = SparkSession.builder().appName(this.getClass.getSimpleName).master("local[*]").getOrCreate()

  test("Test V2MicroBatchDataSource with default config returns a dataset with 25 rows") {
    val rows = 25
    val data = spark.readStream.format("V2MicroBatchDataSource").load
    val query =
      data.
        writeStream.
        outputMode("append").
        format("memory").
        queryName("microbatch1").
        trigger(Trigger.Once()).start()
    query.awaitTermination()
    val result = spark.sql("select * from microbatch1")
    assert(rows == result.count)
  }

  test("Test V2MicroBatchDataSource with one partition returns a dataset with 5 rows") {
    val rows = 5
    val data = spark.readStream.format("V2MicroBatchDataSource").option("partitions", 1).load
    val query =
      data.
        writeStream.
        outputMode("append").
        format("memory").
        queryName("microbatch2").
        trigger(Trigger.Once()).start()
    query.awaitTermination()
    val result = spark.sql("select * from microbatch2")
    assert(rows == result.count)
  }

}
