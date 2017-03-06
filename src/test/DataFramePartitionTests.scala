package test

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterEach, FunSuite}
import org.apache.spark.sql.functions.col

/**
  * Created by ramaharjan on 3/6/17.
  */
class DataFramePartitionTests extends FunSuite with BeforeAndAfterEach {

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

  test("test repartition with column name in dataframe"){
    val sparkContext = sparkSession.sparkContext
    val sqlContext = sparkSession.sqlContext
    import sqlContext.implicits._
    val df1 = sparkContext.parallelize(Vector(("A", 1), ("B", 2), ("A", 3), ("C", 1), ("A", 51), ("B", 1), ("D", 9)))
      .toDF("k1", "v1")
    val df2 = sparkContext.parallelize(Vector(("A", 1), ("B", 2), ("A", 3), ("C", 1), ("A", 51), ("B", 1), ("E", 9)))
      .toDF("k2", "v2")

    //for controlling number of partitions defaults to 200 in shuffling of spark
    sqlContext.setConf("spark.sql.shuffle.partitions", "3")

    val partitioned1 = df1.repartition(col("k1"))
    val partitioned2 = df2.repartition(col("k2"))
    //testing for where the data reside in partitions
    val p1 = partitioned1.rdd.mapPartitionsWithIndex((index, iter) => {if(index == 2) iter else Iterator()})
    val p2 = partitioned2.rdd.mapPartitionsWithIndex((index, iter) => {if(index == 2) iter else Iterator()})

    //printing
    println("first Partition of first table")
    p1.foreach(println)
    println("first Partition of second table")
    p2.foreach(println)
  }

}
