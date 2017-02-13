package main.scala.officework.doingWithClasses

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.functions.udf

import scala.io.Source

/**
  * Created by ramaharjan on 2/6/17.
  */
class MasterTableUdfs(masterTableLocation : String) extends scala.Serializable {

  var masterTableRdd = Source.fromFile(masterTableLocation)
    .getLines()
    .map(line=>line.split("\\|", -1))
    .map(row => (row(1).replace("\"","") -> Array(row(5).replace("\"",""), row(6).replace("\"",""), row(3).replace("\"",""), row(4).replace("\"",""))))
    .toMap

  def updateFromDiagMT(medical : DataFrame): DataFrame = {
    var medicalDiags = medical
    for(i <- 1 to 9) {
      medicalDiags = medicalDiags.withColumn("diag"+i+"_grouper_id", getDiagGrouperId(medicalDiags("svc_diag_"+i+"_code")))
        .withColumn("diag"+i+"_grouper_desc", getDiagGrouperIdDesc(medicalDiags("svc_diag_"+i+"_code")))
        .withColumn("diag"+i+"_supergrouper_id", getSuperDiagGrouperId(medicalDiags("svc_diag_"+i+"_code")))
        .withColumn("diag"+i+"_supergrouper_desc", getsuperDiagGrouperIdDesc(medicalDiags("svc_diag_"+i+"_code")))
    }
    medicalDiags
  }

  def getDiagGrouperId = udf((diagCode : String) => {
    val matchedArray = masterTableRdd.getOrElse(diagCode, "Ungroupable")
    if(matchedArray != "Ungroupable") masterTableRdd(diagCode)(0) else "Ungroupable"
  })

  def getDiagGrouperIdDesc = udf((diagCode : String) => {
    val matchedArray = masterTableRdd.getOrElse(diagCode, "Ungroupable")
    if(matchedArray != "Ungroupable") masterTableRdd(diagCode)(1) else "Ungroupable"
  })

  def getSuperDiagGrouperId = udf((diagCode : String) => {
    val matchedArray = masterTableRdd.getOrElse(diagCode, "Ungroupable")
    if(matchedArray != "Ungroupable") masterTableRdd(diagCode)(2) else "Ungroupable"
  })

  def getsuperDiagGrouperIdDesc = udf((diagCode : String) => {
    val matchedArray = masterTableRdd.getOrElse(diagCode, "Ungroupable")
    if(matchedArray != "Ungroupable") masterTableRdd(diagCode)(3) else "Ungroupable"
  })

  def getFinalString(row : Array[String]): String = {
    var returnString = "Ungroupable"
    for(finalString <- row){
      returnString = finalString
    }
    returnString
  }
}
