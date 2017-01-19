package src.Tests

/**
  * Created by ramaharjan on 1/18/17.
  */
import java.io.File
import java.util.Scanner

import jdk.nashorn.api.scripting.JSObject
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}

import scala.io.Source

object Main {
  def main(args: Array[String]) {
    // val logFile = "file:///home/ramaharjan/Desktop/SparkWordCount.scala" // Should be some file on your system
    val sourceFile = "file:/home/ramaharjan/bugs/110054/elig.csv" // Should be some file on your system
    val layoutFile = "file:/home/ramaharjan/bugs/110054/eligibilityLayout.csv" // Should be some file on your system
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val sc = new SparkContext(conf)

//    layoutTable(sc, layoutFile)
    jsonParser()

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
    sc.stop()
  }

  case class layout(sn : String, name1 : String, name2:String, name3:String, dType:String, parent:String, format:String)
  def layoutTable(sc : SparkContext, file : String): Unit ={
    val readData = sc.textFile(file).filter(!_.startsWith("#"))
    convertJson(readData)
    val mapData = readData.map(x=>x.split(";", -1)).map {y => layout(y(0), y(1), y(2), y(3), y(4), y(5), y(6))}
    val sQLContext = new SQLContext(sc)
    import sQLContext.implicits._
    val dsData = mapData.toDS()
    val scrubName = dsData.select("name1", "dType")
    val dataType = dsData.select("dType")
    for(scrub <- scrubName.rdd){
//      println(scrub(0).toString.replace("\"", "").trim+" : "+scrub(1).toString.replace("\"", "").trim)
    }
  }

  def convertJson(data : RDD[String]): Unit ={
//    def jsonData = data.map(x =>x.split(";", -1)).map(kv => {JsObject(List(kv(1) -> kv(4)))})
//    println(jsonData)
  }

  def jsonParser(): Unit ={
    import scala.util.parsing.json._

    val parsed = JSON.parseFull("""{"Name":"abc", "age":10}""")
    println(parsed)
  }

}
