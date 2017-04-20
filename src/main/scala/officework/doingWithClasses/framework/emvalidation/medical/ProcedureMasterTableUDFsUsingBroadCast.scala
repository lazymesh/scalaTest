package officework.doingWithClasses.framework.emvalidation.medical

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{lit, udf}

import scala.collection.mutable

/**
  * Created by ramaharjan on 2/14/17.
  */
class ProcedureMasterTableUDFsUsingBroadCast(bc : Broadcast[scala.collection.Map[java.lang.String, Array[java.lang.String]]]) extends scala.Serializable {

  val procedureTypeMap = mutable.HashMap[String, String](
    ("svc_procedure_code", ""),
    ("svc_rev_code", "Rev Code"),
    ("svc_cpt_code", "CPT4"),
    ("svc_icd_proc_1_code", "ICD9"),
    ("svc_icd_proc_2_code", "ICD9"),
    ("svc_drg_code", "DRG 27"),
    ("svc_hcpcs_code", "HCPCS")
  )
  val procCodeTypes = Array(
    "",
    "Rev Code",
    "CPT4",
    "ICD9",
    "ICD9",
    "DRG 27",
    "HCPCS")
  val procCodeFields = Array(
    "svc_procedure_code",
    "svc_rev_code",
    "svc_cpt_code",
    "svc_icd_proc_1_code",
    "svc_icd_proc_2_code",
    "svc_drg_code",
    "svc_hcpcs_code")

  def performProcedureMasterTable(medicalDF : DataFrame) : DataFrame = {
    var medicalTempDF = medicalDF
    for (i <- 0 to 6) {
      var j : Int = i+1
      if(i==0){
        medicalTempDF = medicalTempDF.withColumn("svc_procedure_grouper", getProcedureGrouperId(medicalTempDF(procCodeFields(i)), medicalTempDF("svc_procedure_type"), lit(i)))
          .withColumn("svc_procedure_sub_grouper", getSubProcedureGrouperIdDesc(medicalTempDF(procCodeFields(i)), medicalTempDF("svc_procedure_type"), lit(i)))
      }
      else{
        medicalTempDF = medicalTempDF.withColumn("Proc" + j + "_grouper_id", getProcedureGrouperId(medicalTempDF(procCodeFields(i)), medicalTempDF("svc_procedure_type"), lit(i)))
          .withColumn("Proc" + j + "_Subgrouper_desc", getSubProcedureGrouperIdDesc(medicalTempDF(procCodeFields(i)), medicalTempDF("svc_procedure_type"), lit(i)))
      }
      medicalTempDF = medicalTempDF.withColumn("Proc" + j + "_grouper_desc", getProcedureGrouperIdDesc(medicalTempDF(procCodeFields(i)), medicalTempDF("svc_procedure_type"), lit(i)))
        .withColumn("Proc" + j + "_Subgrouper_id", getSubProcedureGrouperId(medicalTempDF(procCodeFields(i)), medicalTempDF("svc_procedure_type"), lit(i)))

    }
    medicalTempDF
  }

  def getProcedureGrouperId = udf((procCode : String, svcProcType : String, loopIterator : Int) => {
    val key : String = getCombinedProcCode(procCode, svcProcType, loopIterator)
    val matchedArray = bc.value.getOrElse(key, "Ungroupable")
    if(matchedArray != "Ungroupable") bc.value(key)(0) else "Ungroupable"
  })

  def getProcedureGrouperIdDesc = udf((procCode : String, svcProcType : String, loopIterator : Int) => {
    val key : String = getCombinedProcCode(procCode, svcProcType, loopIterator)
    val matchedArray = bc.value.getOrElse(key, "Ungroupable")
    if(matchedArray != "Ungroupable") bc.value(key)(1) else "Ungroupable"
  })

  def getSubProcedureGrouperId = udf((procCode : String, svcProcType : String, loopIterator : Int) => {
    val key : String = getCombinedProcCode(procCode, svcProcType, loopIterator)
    val matchedArray = bc.value.getOrElse(key, "Ungroupable")
    if(matchedArray != "Ungroupable") bc.value(key)(2) else "Ungroupable"
  })

  def getSubProcedureGrouperIdDesc = udf((procCode : String, svcProcType : String, loopIterator : Int) => {
    val key : String = getCombinedProcCode(procCode, svcProcType, loopIterator)
    val matchedArray = bc.value.getOrElse(key, "Ungroupable")
    if(matchedArray != "Ungroupable") bc.value(key)(3) else "Ungroupable"
  })

  private def getCombinedProcCode(procCode : String, svcProcType : String, loopIterator : Int) : String = {
    if(loopIterator == 0) {
      procCode+svcProcType
    }
    else {
      procCode + procCodeTypes(loopIterator)
    }
  }
}