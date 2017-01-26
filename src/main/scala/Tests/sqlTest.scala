package src.Tests

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ramaharjan on 1/19/17.
  */
object sqlTest {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val sc = new SparkContext(conf)
    sqlTest(sc)
  }
  case class Table(name : String, age : Int)
  def sqlTest(sc : SparkContext): Unit ={
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._
    var row1 = Seq(Table("name1", 23)).toDS()
    val row2 = Seq(Table("name2", 24)).toDS()

    row1 = row1.union(row2)
    row1.show()
  }

}
