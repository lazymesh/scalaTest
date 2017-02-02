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

  test("updating column testing"){
    val sparkContext = sparkSession.sparkContext
    val sqlContext = sparkSession.sqlContext
    import sqlContext.implicits._
    val df = Seq(("hello"), ("world"),("hello"), ("tree"),("hello"), ("world"),("good"), ("world")).toDF("text")

    df.rdd.map(row => row).collect().foreach(println)
/*    df.map(row => {
      val row1 = row.getAs[String](1)
      val make = if (row1.toLowerCase == "hello") "great" else row1
      Row(make)
    })*/

    sparkContext.stop
  }
}
