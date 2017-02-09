package main.scala.officework.doingWithClasses.masterTableUsingDF

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._

import scala.collection.immutable.HashMap

/**
  * Created by ramaharjan on 2/7/17.
  */
class ProcedureMasterTableUDFs(bc : MasterTableGroupers) extends scala.Serializable{
  val procedureTypeMap = HashMap[String, String](
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
        medicalTempDF = medicalTempDF.withColumn("svc_procedure_grouper", grouperId(medicalTempDF(procCodeFields(i)), medicalTempDF("svc_procedure_type"), lit(i)))
          .withColumn("svc_procedure_sub_grouper", superGrouperIdDesc(medicalTempDF(procCodeFields(i)), medicalTempDF("svc_procedure_type"), lit(i)))
      }
      else{
        medicalTempDF = medicalTempDF.withColumn("Proc" + j + "_grouper_id", grouperId(medicalTempDF(procCodeFields(i)), medicalTempDF("svc_procedure_type"), lit(i)))
          .withColumn("Proc" + j + "_Subgrouper_desc", superGrouperIdDesc(medicalTempDF(procCodeFields(i)), medicalTempDF("svc_procedure_type"), lit(i)))
      }
      medicalTempDF = medicalTempDF.withColumn("Proc" + j + "_grouper_desc", grouperIdDesc(medicalTempDF(procCodeFields(i)), medicalTempDF("svc_procedure_type"), lit(i)))
        .withColumn("Proc" + j + "_Subgrouper_id", superGrouperId(medicalTempDF(procCodeFields(i)), medicalTempDF("svc_procedure_type"), lit(i)))

    }
    medicalTempDF
  }

  def grouperId = udf((procCode : String, svcProcType : String, loopIterator : Int) =>
    getGrouperId(getCombinedProcCode(procCode, svcProcType, loopIterator))
  )

  def grouperIdDesc = udf((procCode : String, svcProcType : String, loopIterator : Int) => {
    bc.getGrouperIdToDiagGrouperDesc.getOrElse(getGrouperId(getCombinedProcCode(procCode, svcProcType, loopIterator)), "Ungroupable")
  }
  )

  def superGrouperId = udf((procCode : String, svcProcType : String, loopIterator : Int) =>
    getSuperGrouperId(getCombinedProcCode(procCode, svcProcType, loopIterator))
  )

  def superGrouperIdDesc = udf((procCode : String, svcProcType : String, loopIterator : Int) =>
    bc.getSuperGrouperIdToSuperGrouperDesc.getOrElse(getSuperGrouperId(getCombinedProcCode(procCode, svcProcType, loopIterator)), "Ungroupable")
  )


  private def getGrouperId(diagCode : String): String = {
    bc.getCodeToDiagGrouperId.getOrElse(diagCode, "Ungroupable")
  }

  private def getSuperGrouperId(diagCode : String): String = {
    bc.getCodeToDiagSuperGrouperId.getOrElse(diagCode, "Ungroupable")
  }

  private def getCombinedProcCode(procCode : String, svcProcType : String, loopIterator : Int) : String = {
    if(loopIterator == 1) {
      procCode+svcProcType
    }
    else {
      procCode + procCodeTypes(loopIterator)
    }
  }

}