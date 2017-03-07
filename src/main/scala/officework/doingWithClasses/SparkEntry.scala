package main.scala.officework.doingWithClasses

import cascading.tuple.{Tuple, Tuples}
import main.scala.officework.ScalaUtils
import main.scala.officework.doingWithClasses.masterTableUsingDF.{DiagnosisMasterTableUDFs, MasterTableGroupers, ProcedureMasterTableUDFs}
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat
import org.apache.spark.SparkFiles
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.collection.JavaConverters._

/**
  * Created by ramaharjan on 2/1/17.
  */
object SparkEntry {

  def main(args: Array[String]) {

    //    val clientId = args(0)+"/"
    val eligJobConfig = new JobCfgParameters("/validation_eligibility.jobcfg")
    val medicalJobConfig = new JobCfgParameters("/emValidation_Medical.jobcfg")

    val clientConfig = new ClientCfgParameters("/client_config.properties")

    //spark configurations
    val sparkSession = SparkSession.builder().appName("Simple Application")
      .master("local")
      .config("", "")
      .getOrCreate()

    //    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val sc = sparkSession.sparkContext
    val sqlContext = sparkSession.sqlContext

    //schema generation for the input source
    val generateSchemas = new GenerateSchemas
    val eligSchema = generateSchemas.dynamicSchema(eligJobConfig.getInputLayoutFilePath)
    val medicalSchema = generateSchemas.dynamicSchema(medicalJobConfig.getInputLayoutFilePath)

    //dataframe creating instance
    val generateDataFrame = new GenerateDataFrame

    //master table dataframes
    val diagnosisMasterTableLocation : String = "/home/ramaharjan/Documents/testProjects/gitHubScala/scalaTest/src/main/resources/Diagnosis.csv"
    sc.addFile(diagnosisMasterTableLocation)
    val diagnosisMasterTableUdfs = new DiagnosisMasterTableUDFs(SparkFiles.get("Diagnosis.csv"))
    //    val diagMasterTableRddMap = sc.textFile(diagnosisMasterTableLocation)
    //      .map(line=>line.split("\\|", -1))
    //      .map(row => row(1).replace("\"","") -> Array(row(5).replace("\"",""), row(6).replace("\"",""), row(3).replace("\"",""), row(4).replace("\"","")))
    //      .collectAsMap()
    //    val diagMTBroadCast = sc.broadcast(diagMasterTableRddMap)
    //    val diagnosisMasterTableUdfs = new DiagnosisMasterTableUDFsUsingBroadCast(diagMTBroadCast)

    val procedureMasterTableLocation : String = "/home/ramaharjan/Documents/testProjects/gitHubScala/scalaTest/src/main/resources/Procedure.csv"
    sc.addFile(procedureMasterTableLocation)
    val procedureMasterTableUdfs = new ProcedureMasterTableUDFs(SparkFiles.get("Procedure.csv"))
    //    val procMasterTableRddMap = sc.textFile(procedureMasterTableLocation)
    //      .map(line=>line.split("\\|", -1))
    //      .map(row => row(1).replace("\"","")+row(4).replace("\"","") -> Array(row(5).replace("\"",""), row(6).replace("\"",""), row(18).replace("\"",""), row(7).replace("\"","")))
    //      .collectAsMap()
    //    val procMTBroadCast = sc.broadcast(procMasterTableRddMap)
    //    val procedureMasterTableUdfs = new ProcedureMasterTableUDFsUsingBroadCast(procMTBroadCast)


    //defining line delimiter for source files
    sc.hadoopConfiguration.set("textinputformat.record.delimiter", "^*~")
    val eligibilityDataRdd = sc.textFile(eligJobConfig.getSourceFilePath)
    val medicalDataRdd = sc.textFile(medicalJobConfig.getSourceFilePath)

    //data frame generation for input source
    var eligibilityTable = generateDataFrame.createDataFrame(sqlContext, eligibilityDataRdd, eligSchema, "\\^%~")
    val medicalTable = generateDataFrame.createDataFrame(sqlContext, medicalDataRdd, medicalSchema, "\\^%~")

    //applying golden rules
    val goldenRules = new GoldenRules(clientConfig.getEOC, clientConfig.getClientType)
    eligibilityTable = goldenRules.applyEligibilityGoldenRules(eligibilityTable)

    //eligibility validation output
    val eligRDD = eligibilityTable.rdd.map(row => row.toString().replace("[","").replace("]",""))
    //    OutputSavingFormatUtils.sequenceTupleFormats(eligRDD, eligJobConfig.getSinkFilePath, ",")
    //    OutputSavingFormatUtils.textCSVFormats(eligRDD, eligJobConfig.getSinkFilePath)
    OutputSavingFormatUtils.dataFrameToCSVFormat(eligibilityTable, eligJobConfig.getSinkFilePath)

/*    val memberIdRdd = eligRDD.map(row => row(1)).distinct.zipWithUniqueId()
    import sqlContext.implicits._
    val memberIdDataFrame = memberIdRdd.toDF("dw_member_id", "intMemberId")*/

    //integer member id output
//    val memberIdRDD = eligibilityTable.select("dw_member_id").distinct().rdd
    val memberId = eligibilityTable.select("dw_member_id").distinct().withColumnRenamed("dw_member_id", "dw_member_id_1")
    memberId.createOrReplaceTempView("simpleTable")
    val memberIdRDD = sqlContext.sql("select row_number() over (order by dw_member_id_1) as int_member_id,dw_member_id_1 from simpleTable")

    val intRDD = memberIdRDD.rdd.map(row => row.toString().replace("[","").replace("]",""))
    //    OutputSavingFormatUtils.sequenceTupleFormats(intRDD, eligJobConfig.getIntMemberId, ",")
    OutputSavingFormatUtils.textCSVFormats(intRDD, eligJobConfig.getIntMemberId)
    //    OutputSavingFormatUtils.dataFrameToCSVFormat(memberIdRDD, eligJobConfig.getIntMemberId)

    val medicalDF = medicalTable.join(memberIdRDD, medicalTable("dw_member_id") === memberIdRDD("dw_member_id_1"), "inner")
    medicalDF.drop(medicalDF.col("dw_member_id_1"))


    var medicalGoldenRulesApplied = goldenRules.applyMedialGoldenRules(medicalDF)

    val procedureFunction = new ProcedureFunction
    medicalGoldenRulesApplied = procedureFunction.performMedicalProcedureType(medicalGoldenRulesApplied)

    //master table
    medicalGoldenRulesApplied = diagnosisMasterTableUdfs.performDiagnosisMasterTable(medicalGoldenRulesApplied)
    medicalGoldenRulesApplied = procedureMasterTableUdfs.performProcedureMasterTable(medicalGoldenRulesApplied)

    val medicalRDD = medicalGoldenRulesApplied.rdd.map(row => row.toString().replace("[","").replace("]",""))
    //    OutputSavingFormatUtils.sequenceTupleFormats(medicalRDD, medicalJobConfig.getSinkFilePath, ",")
    OutputSavingFormatUtils.textCSVFormats(medicalRDD, medicalJobConfig.getSinkFilePath)
    //    OutputSavingFormatUtils.dataFrameToCSVFormat(medicalGoldenRulesApplied, medicalJobConfig.getSinkFilePath)

    //EmValidation of medical
    val pharmacyJobConfig = new JobCfgParameters("/emValidation_Pharmacy.jobcfg")
    val pharmacySchema = generateSchemas.dynamicSchema(pharmacyJobConfig.getInputLayoutFilePath)
    val pharmacyDataRdd = sc.textFile(pharmacyJobConfig.getSourceFilePath)
    var pharmacyTable = generateDataFrame.createDataFrame(sqlContext, pharmacyDataRdd, pharmacySchema, "\\^%~")

    pharmacyTable = pharmacyTable.join(memberIdRDD, pharmacyTable("dw_member_id") === memberIdRDD("dw_member_id_1"), "inner")
    pharmacyTable.drop(pharmacyTable.col("dw_member_id_1"))

    pharmacyTable = goldenRules.applyPharmacyGoldenRules(pharmacyTable)

    val pharmacyRDD = pharmacyTable.rdd.map(row => row.toString().replace("[","").replace("]",""))
    //    OutputSavingFormatUtils.sequenceTupleFormats(pharmacyRDD, pharmacyJobConfig.getSinkFilePath, ",")
    OutputSavingFormatUtils.textCSVFormats(pharmacyRDD, pharmacyJobConfig.getSinkFilePath)
    //    OutputSavingFormatUtils.dataFrameToCSVFormat(pharmacyTable, pharmacyJobConfig.getSinkFilePath)

    //stopping sparkContext
    sc.stop()
  }
}
