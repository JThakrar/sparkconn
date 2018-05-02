
import org.scalatest.FunSuite

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

class V2BatchDataSourceTestSuite extends FunSuite {

  val spark = SparkSession.builder().appName(this.getClass.getSimpleName).master("local[*]").getOrCreate()

  test("Test V2BatchDataSource with default config returns a dataset with 25 rows") {
    val rows = 25
    val data = spark.read.format("V2BatchDataSource").load
    assert(rows == data.count)
  }

  test("Test V2BatchDataSource with one partition returns a dataset with 5 rows") {
    val rows = 5
    val data = spark.read.format("V2BatchDataSource").option("partitions", 1).load
    assert(rows == data.count)
  }

  test("Test V2BatchDataSource with one partition and one row per partition returns a dataset with 1 row") {
    val rows = 1
    val data = spark.read.format("V2BatchDataSource").option("partitions", 1).option("rowsperpartition", 1).load
    assert(rows == data.count)
  }

}
