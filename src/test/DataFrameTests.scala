package test

import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.{BeforeAndAfterEach, FunSuite}

/**
  * Created by ramaharjan on 2/2/17.
  */
class DataFrameTests extends FunSuite with BeforeAndAfterEach {

  var sparkSession : SparkSession = _
  override def beforeEach() {
    sparkSession = SparkSession.builder().appName("udf testings")
      .master("local")
      .config("", "")
      .getOrCreate()

  }

  override def afterEach() {

  }

  test("updating from master table test"){
    val sparkContext = sparkSession.sparkContext
    val sqlContext = sparkSession.sqlContext
    import sqlContext.implicits._
    val diagdf = Seq(("239", "abcd"), ("239.1", "abcd"), ("239.2", "abcd"), ("23.9", "abcd"), ("239.5", "abcd")).toDF("diag1", "diagDesc")
    val masterdf = Seq(("239", "abcd")).toDF("diagCode", "desc1")

    diagdf.show
    masterdf.show

    sparkContext.stop
  }
}
