package test

import main.scala.officework.doingWithClasses.{GenerateDataFrame, GenerateSchemas, MasterTableProperties, MasterTableUdfs}
import main.scala.officework.doingWithClasses.masterTableUsingDF.{DiagnosisMasterTableUDFs, MasterTableGroupers, ProcedureMasterTableUDFs}
import org.apache.spark.SparkFiles
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{BeforeAndAfterEach, FunSuite}

import scala.io.Source

/**
  * Created by ramaharjan on 2/7/17.
  */
class ProcedureMasterTableTests extends FunSuite with BeforeAndAfterEach {

  val masterTableLocation : String = "/home/ramaharjan/Documents/testProjects/gitHubScala/scalaTest/src/main/resources/Procedure.csv"
  val sparkFilesMasterTableLocation : String = "Procedure.csv"
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

  test("1 testing masterTable on Medical Table"){
    val sparkContext = sparkSession.sparkContext
    var medicalProcs = procedureDataCreation

    val masterTableProcedureGroupers = new MasterTableGroupers
    sparkContext.addFile(masterTableLocation)
    val procedureMasterTableUDFs = new ProcedureMasterTableUDFs(SparkFiles.get(sparkFilesMasterTableLocation))

    medicalProcs = procedureMasterTableUDFs.performProcedureMasterTable(medicalProcs)
    medicalProcs.show
    sparkContext.stop()
  }

  def procedureDataCreation : DataFrame = {
    val sQLContext = sparkSession.sqlContext
    import sQLContext.implicits._
    val procedureData = Seq(("ICD10", "06S34ZZ", "06U60JZ", "05VY3DZ", "0BC77ZZ", "0C9J3ZX", "07LG3CZ", "05S10ZZ",
      "svc_procedure_grouper", "Proc1_grouper_desc", "Proc1_Subgrouper_id", "svc_procedure_sub_grouper",
      "Proc2_grouper_id", "Proc2_grouper_desc", "Proc2_Subgrouper_id", "Proc2_Subgrouper_desc",
      "Proc3_grouper_id", "Proc3_grouper_desc", "Proc3_Subgrouper_id", "Proc3_Subgrouper_desc"),
      ("ICD10", "", "0C9J3ZX", "07BB3ZX", "0BC77ZZ", "06U60JZ", "svc_drg_code", "",
        "svc_procedure_grouper", "Proc1_grouper_desc", "Proc1_Subgrouper_id", "svc_procedure_sub_grouper",
        "Proc2_grouper_id", "Proc2_grouper_desc", "Proc2_Subgrouper_id", "Proc2_Subgrouper_desc",
        "Proc3_grouper_id", "Proc3_grouper_desc", "Proc3_Subgrouper_id", "Proc3_Subgrouper_desc"),
      ("svc_procedure_type", "05VY3DZ", "05VY3DZ", "07WN3DZ", "05S10ZZ", "08UR3JZ", "07LG3CZ", "07BB3ZX",
        "svc_procedure_grouper", "Proc1_grouper_desc", "Proc1_Subgrouper_id", "svc_procedure_sub_grouper",
        "Proc2_grouper_id", "Proc2_grouper_desc", "Proc2_Subgrouper_id", "Proc2_Subgrouper_desc",
        "Proc3_grouper_id", "Proc3_grouper_desc", "Proc3_Subgrouper_id", "Proc3_Subgrouper_desc"),
      ("ICD10", "06174AY", "0C9J3ZX", "07LG3CZ", "", "07BB3ZX", "", "",
        "svc_procedure_grouper", "Proc1_grouper_desc", "Proc1_Subgrouper_id", "svc_procedure_sub_grouper",
        "Proc2_grouper_id", "Proc2_grouper_desc", "Proc2_Subgrouper_id", "Proc2_Subgrouper_desc",
        "Proc3_grouper_id", "Proc3_grouper_desc", "Proc3_Subgrouper_id", "Proc3_Subgrouper_desc"))
      .toDF("svc_procedure_type", "svc_procedure_code", "svc_rev_code", "svc_cpt_code", "svc_icd_proc_1_code", "svc_icd_proc_2_code", "svc_drg_code", "svc_hcpcs_code",
        "svc_procedure_grouper", "Proc1_grouper_desc", "Proc1_Subgrouper_id", "svc_procedure_sub_grouper",
        "Proc2_grouper_id", "Proc2_grouper_desc", "Proc2_Subgrouper_id", "Proc2_Subgrouper_desc",
        "Proc3_grouper_id", "Proc3_grouper_desc", "Proc3_Subgrouper_id", "Proc3_Subgrouper_desc")
    procedureData
  }

}
