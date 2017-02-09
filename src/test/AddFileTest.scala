package test

import org.apache.spark.SparkFiles
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterEach, FunSuite}

import scala.io.Source

/**
  * Created by ramaharjan on 2/8/17.
  */
class AddFileTest  extends FunSuite with BeforeAndAfterEach {

  val masterTableLocation : String = "/home/ramaharjan/Documents/testProjects/gitHubScala/scalaTest/src/main/resources/Procedure.csv"
  val resourceMasterTableLocation : String = "/Procedure.csv"

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

  test("add file test"){
    val sc = sparkSession.sparkContext
//    sc.addFile("file://"+masterTableLocation)
    sc.addFile(masterTableLocation)
//    val addedfileData = sc.textFile(SparkFiles.get("Procedure.csv"))
//    println(SparkFiles.get("Procedure.csv"))
    val addfile = Source.fromFile(SparkFiles.get("Procedure.csv"))
    addfile.foreach(print)
  }

}
