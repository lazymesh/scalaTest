package officework.doingWithClasses.framework.emvalidation.medical

import officework.doingWithClasses.framework.goldenrules.GoldenRules
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame

/**
  * Created by ramaharjan on 4/20/17.
  */
class EMMedicalAssembly(dataFrame: DataFrame, goldenRules: GoldenRules, sparkContext: SparkContext) {
  var medicalGoldenRulesApplied = goldenRules.applyMedialGoldenRules(dataFrame)

  val procedureFunction = new ProcedureFunction
  medicalGoldenRulesApplied = procedureFunction.performMedicalProcedureType(medicalGoldenRulesApplied)

  //master table dataframes
  val dfsWorkingDir = FileSystem.get(new Configuration()).getWorkingDirectory

  val diagnosisMasterTableLocation : String = dfsWorkingDir+"/masterTables/Diagnosis.csv"
  //    sc.addFile(diagnosisMasterTableLocation)
  //    val diagnosisMasterTableUdfs = new DiagnosisMasterTableUDFs(SparkFiles.get("Diagnosis.csv"))
  val diagMasterTableRddMap = sparkContext.textFile(diagnosisMasterTableLocation)
    .map(line=>line.split("\\|", -1))
    .map(row => row(1).replace("\"","") -> Array(row(5).replace("\"",""), row(6).replace("\"",""), row(3).replace("\"",""), row(4).replace("\"","")))
    .collectAsMap()
  val diagMTBroadCast = sparkContext.broadcast(diagMasterTableRddMap)
  val diagnosisMasterTableUdfs = new DiagnosisMasterTableUDFsUsingBroadCast(diagMTBroadCast)

  val procedureMasterTableLocation : String = dfsWorkingDir+"/masterTables/Procedure.csv"
  //    sc.addFile(procedureMasterTableLocation)
  //    val procedureMasterTableUdfs = new ProcedureMasterTableUDFs(SparkFiles.get("Procedure.csv"))
  val procMasterTableRddMap = sparkContext.textFile(procedureMasterTableLocation)
    .map(line=>line.split("\\|", -1))
    .map(row => row(1).replace("\"","")+row(4).replace("\"","") -> Array(row(5).replace("\"",""), row(6).replace("\"",""), row(18).replace("\"",""), row(7).replace("\"","")))
    .collectAsMap()
  val procMTBroadCast = sparkContext.broadcast(procMasterTableRddMap)
  val procedureMasterTableUdfs = new ProcedureMasterTableUDFsUsingBroadCast(procMTBroadCast)
  //master table
  medicalGoldenRulesApplied = diagnosisMasterTableUdfs.performDiagnosisMasterTable(medicalGoldenRulesApplied)
  medicalGoldenRulesApplied = procedureMasterTableUdfs.performProcedureMasterTable(medicalGoldenRulesApplied)

}

object EMMedicalAssembly {

  def apply(dataFrame: DataFrame, goldenRules: GoldenRules, sparkContext: SparkContext) : DataFrame = {
//    sparkContext.hadoopConfiguration.set("textinputformat.record.delimiter", "\n")
    val medicalDataFrame = new EMMedicalAssembly(dataFrame, goldenRules, sparkContext)
//    sparkContext.hadoopConfiguration.set("textinputformat.record.delimiter", "^*~")
    medicalDataFrame.medicalGoldenRulesApplied
  }
}
