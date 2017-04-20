package officework.doingWithClasses.framework.emvalidation.medical

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.udf

/**
  * Created by ramaharjan on 2/14/17.
  */
class DiagnosisMasterTableUDFsUsingBroadCast(bc : Broadcast[scala.collection.Map[java.lang.String, Array[java.lang.String]]]) extends scala.Serializable {

  def performDiagnosisMasterTable(medical : DataFrame): DataFrame = {
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
    val matchedArray = bc.value.getOrElse(diagCode, "Ungroupable")
    if(matchedArray != "Ungroupable") bc.value(diagCode)(0) else "Ungroupable"
  })

  def getDiagGrouperIdDesc = udf((diagCode : String) => {
    val matchedArray = bc.value.getOrElse(diagCode, "Ungroupable")
    if(matchedArray != "Ungroupable") bc.value(diagCode)(1) else "Ungroupable"
  })

  def getSuperDiagGrouperId = udf((diagCode : String) => {
    val matchedArray = bc.value.getOrElse(diagCode, "Ungroupable")
    if(matchedArray != "Ungroupable") bc.value(diagCode)(2) else "Ungroupable"
  })

  def getsuperDiagGrouperIdDesc = udf((diagCode : String) => {
    val matchedArray = bc.value.getOrElse(diagCode, "Ungroupable")
    if(matchedArray != "Ungroupable") bc.value(diagCode)(3) else "Ungroupable"
  })
}
