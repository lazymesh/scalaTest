package main.scala.officework.doingWithClasses.masterTableUsingDF

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.udf

/**
  * Created by ramaharjan on 2/6/17.
  */
class DiagnosisMasterTableUDFs(bc : Broadcast[MasterTableGroupers]) extends scala.Serializable{

  def performDiagnosisMasterTable(medicalDataFrame : DataFrame): DataFrame={
    var diagnosedmedicalDataFrame = medicalDataFrame
    for(i <- 1 to 9) {
      diagnosedmedicalDataFrame = diagnosedmedicalDataFrame.withColumn("diag"+i+"_grouper_id", grouperId(diagnosedmedicalDataFrame("svc_diag_"+i+"_code")))
        .withColumn("diag"+i+"_grouper_desc", grouperIdDesc(diagnosedmedicalDataFrame("svc_diag_"+i+"_code")))
        .withColumn("diag"+i+"_supergrouper_id", superGrouperId(diagnosedmedicalDataFrame("svc_diag_"+i+"_code")))
        .withColumn("diag"+i+"_supergrouper_desc", superGrouperIdDesc(diagnosedmedicalDataFrame("svc_diag_"+i+"_code")))
    }
    diagnosedmedicalDataFrame
  }

  def grouperId = udf((diagCode : String) =>
    getGrouperId(diagCode)
  )

  def grouperIdDesc = udf((diagCode : String) =>
    bc.value.getGrouperIdToDiagGrouperDesc.getOrElse(getGrouperId(diagCode), "Ungroupable")
  )

  def superGrouperId = udf((diagCode : String) =>
    getSuperGrouperId(diagCode)
  )

  def superGrouperIdDesc = udf((diagCode : String) =>
    bc.value.getSuperGrouperIdToSuperGrouperDesc.getOrElse(getSuperGrouperId(diagCode), "Ungroupable")
  )


  private def getGrouperId(diagCode : String): String = {
    bc.value.getCodeToDiagGrouperId.getOrElse(diagCode, "Ungroupable")
  }

  private def getSuperGrouperId(diagCode : String): String = {
    bc.value.getCodeToDiagSuperGrouperId.getOrElse(diagCode, "Ungroupable")
  }

}
