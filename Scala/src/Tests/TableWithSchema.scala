package src.Tests

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types._

/**
  * Created by ramaharjan on 1/19/17.
  */
object TableWithSchema {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val sc = new SparkContext(conf)
    testingSchema(sc)
    sc.stop()
  }

  def testingSchema(sc : SparkContext): Unit ={
    val schema = StructType(Seq(StructField("id",IntegerType), StructField("val",StringType)))
    val inputLines = sc.parallelize(Array(Array("1","This is a line for testing"), Array("2","The second line")))
    val rowRdd = inputLines.map{ array => Row.fromSeq(array.zip(schema.toSeq).map{ case (value, struct) => convertTypes(value, struct) })
    }
    val sqlContext = new SQLContext(sc)
    val df = sqlContext.createDataFrame(rowRdd, schema)
    df.show()
  }

  def convertTypes(value: String, struct: StructField): Any = struct.dataType match {
    case BinaryType => value.toCharArray().map(ch => ch.toByte)
    case ByteType => value.toByte
    case BooleanType => value.toBoolean
    case DoubleType => value.toDouble
    case FloatType => value.toFloat
    case ShortType => value.toShort
    case DateType => value
    case IntegerType => value.toInt
    case LongType => value.toLong
    case _ => value
  }

}
