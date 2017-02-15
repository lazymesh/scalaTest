package main.scala.officework.doingWithClasses.masterTableUsingDF

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{udf, _}

import scala.collection.immutable.HashMap
import scala.io.Source

/**
  * Created by ramaharjan on 2/7/17.
  */
class ProcedureMasterTableUDFs(masterTableLocation : String) extends scala.Serializable {

  var procedureMasterTableRdd = Source.fromFile(masterTableLocation)
    .getLines()
    .map(line=>line.split("\\|", -1))
    .map(row => row(1).replace("\"","")+row(4).replace("\"","") -> Array(row(5).replace("\"",""), row(6).replace("\"",""), row(18).replace("\"",""), row(7).replace("\"","")))
    .toMap

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
    val matchedArray = procedureMasterTableRdd.getOrElse(key, "Ungroupable")
    if(matchedArray != "Ungroupable") procedureMasterTableRdd(key)(0) else "Ungroupable"
  })

  def getProcedureGrouperIdDesc = udf((procCode : String, svcProcType : String, loopIterator : Int) => {
    val key : String = getCombinedProcCode(procCode, svcProcType, loopIterator)
    val matchedArray = procedureMasterTableRdd.getOrElse(key, "Ungroupable")
    if(matchedArray != "Ungroupable") procedureMasterTableRdd(key)(1) else "Ungroupable"
  })

  def getSubProcedureGrouperId = udf((procCode : String, svcProcType : String, loopIterator : Int) => {
    val key : String = getCombinedProcCode(procCode, svcProcType, loopIterator)
    val matchedArray = procedureMasterTableRdd.getOrElse(key, "Ungroupable")
    if(matchedArray != "Ungroupable") procedureMasterTableRdd(key)(2) else "Ungroupable"
  })

  def getSubProcedureGrouperIdDesc = udf((procCode : String, svcProcType : String, loopIterator : Int) => {
    val key : String = getCombinedProcCode(procCode, svcProcType, loopIterator)
    val matchedArray = procedureMasterTableRdd.getOrElse(key, "Ungroupable")
    if(matchedArray != "Ungroupable") procedureMasterTableRdd(key)(3) else "Ungroupable"
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