package test

import main.scala.officework.doingWithClasses._
import main.scala.officework.doingWithClasses.masterTableUsingDF.{DiagnosisMasterTableUDFs, ProcedureMasterTableUDFs}
import org.apache.spark.SparkFiles
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalatest.{BeforeAndAfterEach, FunSuite}
import org.apache.spark.sql.functions._

/**
  * Created by ramaharjan on 2/2/17.
  */
class DataFrameTests extends FunSuite with BeforeAndAfterEach {

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

  test("updating a table using another tables value "){
    val sparkContext = sparkSession.sparkContext
    val sqlContext = sparkSession.sqlContext
    import sqlContext.implicits._
    var diagdf = Seq(("239", "", "1"), ("239.1", "", "2"), ("239.2", "", "3"), ("23.9", "", "4"), ("239.5", "", "4")).toDF("diag1", "diagDesc", "diag2")
    val masterdf = Seq(("239", "dfsadf"), ("239.1", "dfsdf"), ("239.2", "sdfs"), ("23.9", "dfadf"), ("239.5", "dddd")).toDF("diagCode", "desc1")

    diagdf = diagdf.join(masterdf, diagdf("diag1") === masterdf("diagCode"), "left")
      .withColumn("diagDesc", masterdf("desc1"))
      .drop("diagCode", "desc1")
    diagdf.show

    sparkContext.stop
  }

  test("update a dataframe using another dataframe using join"){
    val sparkContext = sparkSession.sparkContext
    val sqlContext = sparkSession.sqlContext
    import sqlContext.implicits._
    // select only diag codes from medical table
    var diagCodes = Seq(
      ("0", "1", "2", "3", "4", "0", "6", "3", "9"),
      ("1", "3", "4", "5", "1", "3", "4", "5", "1"),
      ("2", "6", "6", "8", "2", "6", "6", "8", "2"),
      ("3", "9", "9", "1", "3", "9", "9", "1", "3"))
      .toDF("svc_diag_1_code", "svc_diag_2_code", "svc_diag_3_code", "svc_diag_4_code", "svc_diag_5_code",
        "svc_diag_6_code", "svc_diag_7_code", "svc_diag_8_code", "svc_diag_9_code")

    //select only groupers and supergroupers with diagcode from master table
    val masterdf = Seq(
      ("0", "", "grouperDescription0", "superGrouperID0", "superGrouperDescription0"),
      ("1", "", "grouperDescription1", "superGrouperID1", "superGrouperDescription1"),
      ("2", "grouperID2", "grouperDescription2", "superGrouperID2", "superGrouperDescription2"),
      ("3", "grouperID3", "grouperDescription3", "superGrouperID3", "superGrouperDescription3"),
      ("4", "grouperID4", "grouperDescription4", "superGrouperID4", "superGrouperDescription4"),
      ("5", "grouperID5", "grouperDescription5", "superGrouperID5", "superGrouperDescription5"),
      ("6", "grouperID6", "grouperDescription6", "superGrouperID6", "superGrouperDescription6"),
      ("7", "grouperID7", "grouperDescription7", "superGrouperID7", "superGrouperDescription7"),
      ("8", "grouperID8", "grouperDescription8", "superGrouperID8", "superGrouperDescription8"),
      ("9", "grouperID9", "grouperDescription9", "superGrouperID9", "superGrouperDescription9"))
      .toDF("diagnosisCode", "grouperID", "grouperDescription", "superGrouperID", "superGrouperDescription")

    def diagnosisMasterTableUdfs = udf((value : String) =>{
      if(!value.isEmpty) value else "Ungroupable"
    })
     for(i <- 1 to 9){
       diagCodes = diagCodes.join(masterdf, diagCodes("svc_diag_"+i+"_code") === masterdf("diagnosisCode"), "left")
         .withColumn("diag"+i+"_grouper_id", diagnosisMasterTableUdfs(masterdf("grouperID")))
         .withColumn("diag"+i+"_grouper_desc", diagnosisMasterTableUdfs(masterdf("grouperDescription")))
         .withColumn("diag"+i+"_supergrouper_id", diagnosisMasterTableUdfs(masterdf("superGrouperID")))
         .withColumn("diag"+i+"_supergrouper_desc", diagnosisMasterTableUdfs(masterdf("superGrouperDescription")))
      .drop("diagnosisCode", "grouperID", "grouperDescription", "superGrouperID", "superGrouperDescription")
     }
    sparkContext.stop()
  }

  test("testing update on actual data") {
    val sparkContext = sparkSession.sparkContext
    val sqlContext = sparkSession.sqlContext

    val masterTableLocation: String = "/home/ramaharjan/Documents/testProjects/gitHubScala/scalaTest/src/main/resources/Diagnosis.csv"
    val medicalJobConfig = new JobCfgParameters("/emValidation_Medical.jobcfg")
    val generateSchemas = new GenerateSchemas
    val medicalSchema = generateSchemas.dynamicSchema(medicalJobConfig.getInputLayoutFilePath)

    val masterTableSchema = generateSchemas.dynamicSchema("/diagnosisLayout.csv")
    val masterTableDiagRdd = sparkContext.textFile(masterTableLocation)

    val generateDataFrame = new GenerateDataFrame

    var masterTableDF = generateDataFrame.createMasterDataFrame(sqlContext, masterTableDiagRdd, masterTableSchema)

    sparkContext.hadoopConfiguration.set("textinputformat.record.delimiter", "^*~")
    val medicalDataRdd = sparkContext.textFile(medicalJobConfig.getSourceFilePath)

    var medicalTable = generateDataFrame.createDataFrame(sqlContext, medicalDataRdd, medicalSchema)
    val masterTableLocation2: String = "Diagnosis.csv"

    masterTableDF = masterTableDF.select("diagnosisCode", "grouperID", "grouperDescription", "superGrouperID", "superGrouperDescription")

    val masterTableBC = sparkContext.broadcast(masterTableDF)
    val masterTableUdfs = new MasterTableUdfs(masterTableBC)

    medicalTable = updateFromDiagnosisMT(medicalTable)
    medicalTable.show
    def diagnosisMasterTableUdfs = udf((value: String) => {
      if (value != null) value else "Ungroupable"
    })

    def updateFromDiagnosisMT(medicalTable: DataFrame): DataFrame = {
      var updatedMedical = medicalTable
      for (i <- 1 to 9) {
        updatedMedical = updatedMedical.join(masterTableBC.value, updatedMedical("svc_diag_" + i + "_code") === masterTableBC.value("diagnosisCode"), "left")
          .withColumn("diag" + i + "_grouper_id", diagnosisMasterTableUdfs(masterTableBC.value("grouperID")))
          .withColumn("diag" + i + "_grouper_desc", diagnosisMasterTableUdfs(masterTableBC.value("grouperDescription")))
          .withColumn("diag" + i + "_supergrouper_id", diagnosisMasterTableUdfs(masterTableBC.value("superGrouperID")))
          .withColumn("diag" + i + "_supergrouper_desc", diagnosisMasterTableUdfs(masterTableBC.value("superGrouperDescription")))
          .drop("diagnosisCode", "grouperID", "grouperDescription", "superGrouperID", "superGrouperDescription")
      }
      updatedMedical
    }
  }

  test("merging/union testing of two dataframes of different size"){
    var firstTableColumns = Seq("diag1", "diagDesc", "diag2")
    var firstTableNextColumns = Seq("diag2")
    var secondTableColumns = Seq("diagCode", "desc1")
    var finalSecond = secondTableColumns ++ firstTableNextColumns
    val sparkContext = sparkSession.sparkContext
    val sqlContext = sparkSession.sqlContext
    import sqlContext.implicits._
    var diagdf = Seq(("239", "", "1"), ("239.1", "", "2"), ("239.2", "", "3"), ("23.9", "", "4"), ("239.5", "", "4")).toDF(firstTableColumns:_*)
    diagdf.createOrReplaceTempView("firstTable")
    val masterdf = Seq(("239", "dfsadf"), ("239.1", "dfsdf"), ("239.2", "sdfs"), ("23.9", "dfadf"), ("239.5", "dddd")).toDF(secondTableColumns: _*)
    masterdf.createOrReplaceTempView("secondTable")

//    diagdf = diagdf.select(firstTableColumns.map(col):_*).union(masterdf.select(secondTableColumns.map(col):_*).("null as diag2"))
    diagdf = sqlContext.sql("select * from firstTable Union select *, null as diag2 from secondTable")

    diagdf.show

    sparkContext.stop
  }

  test("testing for number of tasks when data frame is converted to rdd"){
    val sparkContext = sparkSession.sparkContext
    val sqlContext = sparkSession.sqlContext

    val medicalDataFileLocation = "/home/ramaharjan/Documents/testProjects/gitHubScala/scalaTest/src/main/resources/Medical.csv"
    val medicalJobConfig = new JobCfgParameters("/emValidation_Medical.jobcfg")

    val generateSchemas = new GenerateSchemas
    val medicalSchema = generateSchemas.dynamicSchema(medicalJobConfig.getInputLayoutFilePath)

    sparkContext.hadoopConfiguration.set("textinputformat.record.delimiter", "^*~")
    val medicalDataRdd = sparkContext.textFile(medicalDataFileLocation)

    val generateDataFrame = new GenerateDataFrame
    val medicalTable = generateDataFrame.createDataFrame(sqlContext, medicalDataRdd, medicalSchema)

    val goldenRules = new GoldenRules("2016-12-31", "medicare")
    var medicalGoldenRulesApplied = goldenRules.applyMedialGoldenRules(medicalTable)

    val procedureFunction = new ProcedureFunction
    medicalGoldenRulesApplied = procedureFunction.performMedicalProcedureType(medicalGoldenRulesApplied)

    val diagnosisMasterTableLocation : String = "/home/ramaharjan/Documents/testProjects/gitHubScala/scalaTest/src/main/resources/Diagnosis.csv"
    sparkContext.addFile(diagnosisMasterTableLocation)
    val diagnosisMasterTableUdfs = new DiagnosisMasterTableUDFs(SparkFiles.get("Diagnosis.csv"))
    medicalGoldenRulesApplied = diagnosisMasterTableUdfs.performDiagnosisMasterTable(medicalGoldenRulesApplied)

    val procedureMasterTableLocation : String = "/home/ramaharjan/Documents/testProjects/gitHubScala/scalaTest/src/main/resources/Procedure.csv"
    sparkContext.addFile(procedureMasterTableLocation)
    val procedureMasterTableUdfs = new ProcedureMasterTableUDFs(SparkFiles.get("Procedure.csv"))
    medicalGoldenRulesApplied = procedureMasterTableUdfs.performProcedureMasterTable(medicalGoldenRulesApplied)

    val medicalRDD = medicalGoldenRulesApplied.rdd.map(row => row.toString().replace("[","").replace("]",""))
    OutputSavingFormatUtils.textCSVFormats(medicalRDD, medicalJobConfig.getSinkFilePath)

    val pharmacyJobConfig = new JobCfgParameters("/emValidation_Pharmacy.jobcfg")
    val pharmacySchema = generateSchemas.dynamicSchema(pharmacyJobConfig.getInputLayoutFilePath)
    val pharmacyDataRdd = sparkContext.textFile(pharmacyJobConfig.getSourceFilePath)
    var pharmacyTable = generateDataFrame.createDataFrame(sqlContext, pharmacyDataRdd, pharmacySchema)

    pharmacyTable = goldenRules.applyPharmacyGoldenRules(pharmacyTable)

    val pharmacyRDD = pharmacyTable.rdd.repartition(2).map(row => row.toString().replace("[","").replace("]",""))
    OutputSavingFormatUtils.textCSVFormats(pharmacyRDD, pharmacyJobConfig.getSinkFilePath)
  }
}
