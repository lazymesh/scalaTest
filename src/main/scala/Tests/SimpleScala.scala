package src.Tests

import org.apache.spark.SparkContext

import scala.io.Source

/**
  * Created by ramaharjan on 1/19/17.
  */
object SimpleScala {

  def main(args: Array[String]): Unit = {

  }


  def readFromSource(file: String): Unit ={
    for (line <- Source.fromFile(file).getLines) {
      println(line)
    }
  }

  def wordCount(file : String, sc : SparkContext): Unit ={
    val fileData = sc.textFile(file)
    val count = fileData.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)

    /* saveAsTextFile method is an action that effects on the RDD */
    count.saveAsTextFile("outfile")
  }

  def moreTests(): Unit ={
    /*val sqlContext = new SQLContext(sc);
val df = sqlContext.read.format("com.databricks.spark.csv").option("inferSchema", "false").option("header", "true")
      .option("Delimiter", "~").load(logFile)
println(df.show())*/
    // val numAs = logData.filter(line => line.contains("a")).count()
    // val numBs = logData.filter(line => line.contains("b")).count()
    // println(s"Lines with a: numAs, Lines with b: numBs")
  }
}
