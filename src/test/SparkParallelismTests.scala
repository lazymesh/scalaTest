package test

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterEach, FunSuite}

/**
  * Created by anahcolus on 2/25/17.
  */
class SparkParallelismTests extends FunSuite with BeforeAndAfterEach {

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

  test("simple parallelism"){
    val sc = sparkSession.sparkContext
    val input : String = "four score and seven years ago our fathers "+
      "brought forth on this continent "+
      "a new nation conceived in liberty and "+
      "dedicated to the propostion that all men are created equal"
    val lst = input.split(" ")

    for (i <- 1 to 30) {
      val rdd = sc.parallelize(lst, i);
      println(rdd.partitions.size);
    }
  }

}
