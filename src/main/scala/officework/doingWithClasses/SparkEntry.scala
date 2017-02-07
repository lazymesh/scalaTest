package main.scala.officework.doingWithClasses

import cascading.tuple.{Tuple, Tuples}
import main.scala.officework.ScalaUtils
import main.scala.officework.doingWithClasses.masterTableUsingDF.{DiagnosisMasterTableUDFs, MasterTableGroupers, ProcedureMasterTableUDFs}
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat
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

    val masterTableLocation : String = "/Diagnosis.csv"

    val masterTableDiagnosisGroupers = new MasterTableGroupers
    masterTableDiagnosisGroupers.diagnosisMasterTableToMap(masterTableLocation)
    val broadCastedDiagMT = sc.broadcast(masterTableDiagnosisGroupers)

    val masterTableProcedureGroupers = new MasterTableGroupers
    val procedureMasterTableLocation : String = "/Procedure.csv"
    masterTableProcedureGroupers.procedureMasterTableToMap(procedureMasterTableLocation)
    val broadCastedProcMT = sc.broadcast(masterTableProcedureGroupers)

    //defining line delimiter for source files
    sc.hadoopConfiguration.set("textinputformat.record.delimiter", "^*~")
    val eligibilityDataRdd = sc.textFile(eligJobConfig.getSourceFilePath)
    val medicalDataRdd = sc.textFile(medicalJobConfig.getSourceFilePath)

    //data frame generation for input source
    val generateDataFrame = new GenerateDataFrame
    val eligibilityTable = generateDataFrame.createDataFrame(sqlContext, eligibilityDataRdd, eligSchema)
    val medicalTable = generateDataFrame.createDataFrame(sqlContext, medicalDataRdd, medicalSchema)

    //applying golden rules
    val goldenRules = new GoldenRules(clientConfig.getEOC, clientConfig.getClientType)
    val eligibilityGoldenRulesApplied = goldenRules.applyEligibilityGoldenRules(eligibilityTable)

    //deleting the outputs if they exists
    ScalaUtils.deleteResource(eligJobConfig.getSinkFilePath)
    ScalaUtils.deleteResource(eligJobConfig.getIntMemberId)

    //eligibility validation output
    val eligRDD = eligibilityGoldenRulesApplied.rdd.map(row => row.toString())
    val eligibilityOutput = eligRDD.coalesce(1)
      .map(row => row.toString().split(",").toList.asJava)
      .map(v => (Tuple.NULL, Tuples.create(v.asInstanceOf[java.util.List[AnyRef]])))
      .saveAsNewAPIHadoopFile(eligJobConfig.getSinkFilePath, classOf[Tuple], classOf[Tuple], classOf[SequenceFileOutputFormat[Tuple, Tuple]], ScalaUtils.getHadoopConf)

    //integer member id output
//    val memberIdRDD = eligibilityTable.select("dw_member_id").distinct().rdd
    val memberId = eligibilityTable.select("dw_member_id").distinct().withColumnRenamed("dw_member_id", "dw_member_id_1")
    memberId.createOrReplaceTempView("simpleTable")
    val memberIdRDD = sqlContext.sql("select row_number() over (order by dw_member_id_1) as int_member_id,dw_member_id_1 from simpleTable")

    val intRDD = memberIdRDD.rdd.coalesce(1)
      .map(kv => (Tuple.NULL, new Tuple(kv(0).toString, kv(1).toString)))
      .saveAsNewAPIHadoopFile(eligJobConfig.getIntMemberId, classOf[Tuple], classOf[Tuple], classOf[SequenceFileOutputFormat[Tuple, Tuple]], ScalaUtils.getHadoopConf)

    val medicalDF = medicalTable.join(memberIdRDD, medicalTable("dw_member_id") === memberIdRDD("dw_member_id_1"), "inner")
    medicalDF.drop(medicalDF.col("dw_member_id_1"))


    var medicalGoldenRulesApplied = goldenRules.applyMedialGoldenRules(medicalDF)

    val procedureFunction = new ProcedureFunction
    medicalGoldenRulesApplied = procedureFunction.performMedicalProcedureType(medicalGoldenRulesApplied)

    val diagnosisMasterTableUDFs = new DiagnosisMasterTableUDFs(broadCastedDiagMT)
    medicalGoldenRulesApplied = diagnosisMasterTableUDFs.performDiagnosisMasterTable(medicalGoldenRulesApplied)

    val procedureMasterTableUDFs = new ProcedureMasterTableUDFs(broadCastedProcMT)
    medicalGoldenRulesApplied = procedureMasterTableUDFs.performProcedureMasterTable(medicalGoldenRulesApplied)

    ScalaUtils.deleteResource(medicalJobConfig.getSinkFilePath)

    val medicalRDD = medicalGoldenRulesApplied.rdd.map(row => row.toString())
    val medicalEMValidationOutput = medicalRDD.coalesce(1)
      .map(row => row.toString().split(",").toList.asJava)
      .map(v => (Tuple.NULL, Tuples.create(v.asInstanceOf[java.util.List[AnyRef]])))
      .saveAsNewAPIHadoopFile(medicalJobConfig.getSinkFilePath, classOf[Tuple], classOf[Tuple], classOf[SequenceFileOutputFormat[Tuple, Tuple]], ScalaUtils.getHadoopConf)

    //EmValidation of medical
    val pharmacyJobConfig = new JobCfgParameters("/emValidation_Pharmacy.jobcfg")
    val pharmacySchema = generateSchemas.dynamicSchema(pharmacyJobConfig.getInputLayoutFilePath)
    val pharmacyDataRdd = sc.textFile(pharmacyJobConfig.getSourceFilePath)
    var pharmacyTable = generateDataFrame.createDataFrame(sqlContext, pharmacyDataRdd, pharmacySchema)

    pharmacyTable = pharmacyTable.join(memberIdRDD, pharmacyTable("dw_member_id") === memberIdRDD("dw_member_id_1"), "inner")
    pharmacyTable.drop(pharmacyTable.col("dw_member_id_1"))

    pharmacyTable = goldenRules.applyPharmacyGoldenRules(pharmacyTable)

    ScalaUtils.deleteResource(pharmacyJobConfig.getSinkFilePath)

    val pharmacyRDD = pharmacyTable.rdd.map(row => row.toString())
    val pharmacyEMValidationOutput = pharmacyRDD.coalesce(1)
      .map(row => row.toString().split(",").toList.asJava)
      .map(v => (Tuple.NULL, Tuples.create(v.asInstanceOf[java.util.List[AnyRef]])))
      .saveAsNewAPIHadoopFile(pharmacyJobConfig.getSinkFilePath, classOf[Tuple], classOf[Tuple], classOf[SequenceFileOutputFormat[Tuple, Tuple]], ScalaUtils.getHadoopConf)

    //stopping sparkContext
    sc.stop()
  }
}
