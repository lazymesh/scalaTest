package Scala.src.Tests

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions.udf

/**
  * Created by anahcolus on 1/21/17.
  */
object TestUdf {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val df = sc.parallelize(Seq((1L, 3.0, "a"), (2L, -1.0, "b"), (3L, 0.0, "c"))).toDF("x", "y", "z")

    val df1 = df.withColumn("foobar", foobarUdf($"x", $"y", $"z"))
    df1.show
  }

  case class Foobar(foo: Double, bar: Double)

  val foobarUdf = udf((x: Long, y: Double, z: String) => "a")

}
