package src.Tests

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import src.Tests.Main.layout

/**
  * Created by ramaharjan on 1/19/17.
  */
object CreateLayoutTable {

  def main(args: Array[String]): Unit = {
    val layoutFile = "file:/home/ramaharjan/bugs/110054/eligibilityLayout.csv" // Should be some file on your system
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val sc = new SparkContext(conf)
    layoutTable(sc, layoutFile)
    sc.stop()
  }

  case class layout(sn : String, name1 : String, name2:String, name3:String, dType:String, parent:String, format:String)
  def layoutTable(sc : SparkContext, file : String): Unit ={
    val readData = sc.textFile(file).filter(!_.startsWith("#"))
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
}
