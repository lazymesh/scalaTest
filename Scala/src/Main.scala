/**
  * Created by ramaharjan on 1/18/17.
  */
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SQLContext, SparkSession}

import scala.io.Source
import java.io.File
import java.util.Scanner

import scala.collection.immutable.HashMap

object Main {
  def main(args: Array[String]) {
    // val logFile = "file:///home/ramaharjan/Desktop/SparkWordCount.scala" // Should be some file on your system
    val sourceFile = "file:/home/ramaharjan/bugs/110054/elig.csv" // Should be some file on your system
    val layoutFile = "file:/home/ramaharjan/bugs/110054/eligibilityLayout.csv" // Should be some file on your system
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val sc = new SparkContext(conf)



    /*    sc.hadoopConfiguration.set("textinputformat.record.delimiter","^*~")
        val logData = sc.textFile(sourceFile)

        val rddData = logData.map(_.split("\\^%~"))
        val rowRDD = sc.parallelize(Seq(rddData))

        val layoutData = sc.textFile(layoutFile)
        val sQLContext = new SQLContext(sc)
        import sQLContext.implicits._
        val layout = layoutData.flatMap(_.split(";")).toJavaRDD()
    //    layout.foreach(print)
    //    val reqLayout = layout.
        for(layoutLine <- layout){
          print(layoutLine)
        }*/


    //    layoutData.foreach(val layout = layoutData.map(_.split(";")).map(array => (array(1), array(4))))
    //    val layout = layoutData.map(_.split(";")).map(array => (array(1), array(4)))
    //    layout.foreach(println)

    //    val sQLContext = new SQLContext(sc)
    //    import sQLContext.implicits._
    //    val sql = sQLContext.createDataset(rowRDD)
    //    rddData.toDS().show()
    //    sql.show()
    //    rowRDD.foreach(println)


    /*val sqlContext = new SQLContext(sc);
    val df = sqlContext.read.format("com.databricks.spark.csv").option("inferSchema", "false").option("header", "true")
          .option("Delimiter", "~").load(logFile)
    println(df.show())*/
    // val numAs = logData.filter(line => line.contains("a")).count()
    // val numBs = logData.filter(line => line.contains("b")).count()
    // println(s"Lines with a: numAs, Lines with b: numBs")



    sc.stop()
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

  def splitting(): Unit ={
    val textString = "hello^*~world"
    val splitText = textString.split("\\^\\*~")
    splitText.foreach(println)
  }

  def readAndSplit(file : String, sc : SparkContext): Unit ={
    val fileData = sc.textFile(file).cache()
    val lineData = fileData.map(x =>x.split("\\^\\*~"))
    val yourRdd = lineData.flatMap( arr => {
      val title = arr( 0 )
      val text = arr( 1 )
      val words = title.split( "\\^%~" )
      words.map( word => ( word ) )
    } )
    // RDD[ ( String ) ]

    // Now, if you want to print this...
    yourRdd.foreach( { case ( word ) => println( s"$word" ) } )
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

  case class layout(sn : String, name1 : String, name2:String, name3:String, dType:String)
  def layoutTable(sc : SparkContext, file : String): Unit ={
    val readData = sc.textFile(file)
    val mapData = readData.map(x=>x.split(";")).map {y => layout(y(0), y(1), y(2), y(3), y(4))}
    val sQLContext = new SQLContext(sc)
    import sQLContext.implicits._
    return mapData.toDS()
  }


}
