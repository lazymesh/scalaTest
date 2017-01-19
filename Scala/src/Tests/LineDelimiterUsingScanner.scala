package src.Tests

import java.io.File
import java.util.Scanner

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * Created by ramaharjan on 1/19/17.
  */
object LineDelimiterUsingScanner {

  def main(args: Array[String]): Unit = {
    lineDelimiterUsingScanner()
  }


  def lineDelimiterUsingScanner() : Unit = {
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val sc = new SparkContext(conf)
    val file = new File("/home/ramaharjan/bugs/110054/elig.csv")
    val lineDelimiter = "\\^\\*~"
    val scanner = new Scanner(file).useDelimiter(lineDelimiter)
    val readWithLineDelimiter = collection.JavaConversions.asScalaIterator(scanner)
    val rddData = readWithLineDelimiter.map(_.split("\\^%~"))
    val rowRDD = sc.parallelize(Seq(rddData.toList))

    val sQLContext = new SQLContext(sc)
    import sQLContext.implicits._
    val sql = sQLContext.createDataset(rowRDD)
    sql.show()
  }

}
