package src.Tests

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ramaharjan on 1/19/17.
  */
object JobConfig {

  def main(args: Array[String]) {
    // val logFile = "file:///home/ramaharjan/Desktop/SparkWordCount.scala" // Should be some file on your system
    val jobConfig = "file:/home/ramaharjan/ScalaTest/eligibility_validation.jobcfg"
    // Should be some file on your system
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val sc = new SparkContext(conf)
    jobConfigTable(sc, jobConfig)
  }

  case class jobConfig(key : String, value : String)
  def jobConfigTable(sc : SparkContext, file : String): Unit ={
    val readData = sc.textFile(file)
    val filteredLines = readData.filter(!_.startsWith("#"))
    val nonEmptyLines = readData.filter(!_.isEmpty)
    val mapData = filteredLines.map(line=>line.split(";")).map(kv => jobConfig(kv(0).trim, kv(1).trim))
    val sQLContext = new SQLContext(sc)
    import sQLContext.implicits._
    val jobCfgDS = mapData.toDS()
    jobCfgDS.show()
  }
}
