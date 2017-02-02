package test

import org.apache.spark.sql.{SparkSession}
import org.scalatest.{BeforeAndAfterEach, FunSuite}
import org.apache.spark.sql.functions.udf

import scala.io.Source

/**
  * Created by ramaharjan on 2/2/17.
  */
class UDFTests extends FunSuite with BeforeAndAfterEach {

  var sparkSession : SparkSession = _
  override def beforeEach() {
    sparkSession = SparkSession.builder().appName("udf testings")
      .master("local")
      .config("", "")
      .getOrCreate()

  }

  override def afterEach() {

  }

  test("auto increament udf test with distinct function and renaming column"){
    val sparkContext = sparkSession.sparkContext
    val sqlContext = sparkSession.sqlContext
    import sqlContext.implicits._
    val dataset = Seq(("hello"), ("world"),("hello"), ("tree"),("hello"), ("world"),("good"), ("world")).toDF("text")
    val distinctDataSet = dataset.distinct().withColumnRenamed("text", "names")

//    import org.apache.spark.sql.functions._
//    distinctDataSet.withColumn("uniqueID",monotonically_increasing_id).show()

    distinctDataSet.createOrReplaceTempView("simpleTable")

    val tmpTable = sqlContext.sql("select row_number() over (order by names) as rnk,names from simpleTable")
    tmpTable.show()

/*
    dataset.createOrReplaceTempView("simpleTable")

    val tmpTable = sqlContext.sql("select row_number() over (partition by id order by text) as rnk,id,text from simpleTable")
    tmpTable.show()
*/

    sparkContext.stop()
  }

  def testUdf = udf((value: String, toValue : String) => if(value.equalsIgnoreCase("hello")) toValue else value)
  test("udf text to update a column by passing a constant"){
    val sparkContext = sparkSession.sparkContext
    val sqlContext = sparkSession.sqlContext
    import sqlContext.implicits._
    val df = Seq(("hello"), ("world"),("hello"), ("tree"),("hello"), ("world"),("good"), ("world")).toDF("text")
    import org.apache.spark.sql.functions._
    df.withColumn("text", testUdf(df("text"), lit("Great"))).show

    sparkContext.stop()
  }
}
