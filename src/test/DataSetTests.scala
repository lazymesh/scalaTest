package test

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.aggregate.Count
import org.scalatest.{BeforeAndAfterEach, FunSuite}
import org.apache.spark.sql.functions.{sum, count}

/**
  * Created by ramaharjan on 3/4/17.
  */
class DataSetTests extends FunSuite with BeforeAndAfterEach {

  var sparkSession : SparkSession = _
  override def beforeEach() {
    sparkSession = SparkSession.builder().appName("udf testings")
      .master("local")
      .config("", "")
      .getOrCreate()
  }

  override def afterEach() {
    sparkSession.stop()
  }

  test("simple data set creation and testing"){
    val sparkContext = sparkSession.sparkContext
    val sqlContext = sparkSession.sqlContext
    import sqlContext.implicits._
    val data = sqlContext.read.text("src/main/resources/Eligibility.csv").as[String]

    data.show

    sparkContext.stop()
  }

  test("aggregation on a dataset") {
    val sparkContext = sparkSession.sparkContext
    val sqlContext = sparkSession.sqlContext
    import sqlContext.implicits._
    val ds = Seq((1, 1, 2L), (1, 2, 3L), (1, 3, 4L), (2, 1, 5L)).toDS()
    ds.groupBy("_1").agg(count("*"),sum("_2"), sum("_3")).show

    sparkContext.stop
  }
}
