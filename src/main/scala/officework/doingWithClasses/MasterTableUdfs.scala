package main.scala.officework.doingWithClasses

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.udf

/**
  * Created by ramaharjan on 2/6/17.
  */
class MasterTableUdfs(bcMasterTable : Broadcast[DataFrame]) extends scala.Serializable {


  def getDiagGrouperId = udf((diagCode : String) => {
    val masterTable = bcMasterTable.value
    val diagGrouperId = masterTable.where(masterTable("diagnosisCode") === diagCode).select("grouperID").take(1).map{row => row.getString(0)}
    getFinalString(diagGrouperId)
  })

  def getDiagGrouperIdDesc = udf((diagCode : String) => {
    val masterTable = bcMasterTable.value
    val diagGrouperIdDesc = masterTable.where(masterTable("diagnosisCode") === diagCode).select("grouperDescription").take(1).map{row => row.getString(0)}
    getFinalString(diagGrouperIdDesc)
  })

  def getSuperDiagGrouperId = udf((diagCode : String) => {
    val masterTable = bcMasterTable.value
    val superDiagGrouperId = masterTable.where(masterTable("diagnosisCode") === diagCode).select("superGrouperID").take(1).map{row => row.getString(0)}
    getFinalString(superDiagGrouperId)
  })

  def getsuperDiagGrouperIdDesc = udf((diagCode : String) => {
    val masterTable = bcMasterTable.value
    val superDiagGrouperIdDesc = masterTable.where(masterTable("diagnosisCode") === diagCode).select("superGrouperDescription").take(1).map{row => row.getString(0)}
    getFinalString(superDiagGrouperIdDesc)
  })

  def getFinalString(row : Array[String]): String = {
    var returnString = "Ungroupable"
    for(finalString <- row){
      returnString = finalString
    }
    returnString
  }
}
