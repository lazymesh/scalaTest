package test

import main.scala.officework.doingWithClasses._
import main.scala.officework.doingWithClasses.masterTableUsingDF.{DiagnosisMasterTableUDFs, MasterTableDiagnosisGroupers}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{BeforeAndAfterEach, FunSuite}

import scala.io.Source

/**
  * Created by ramaharjan on 2/3/17.
  */
class MasterTablePropertiesTests extends FunSuite with BeforeAndAfterEach {

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

  test("testing to read from mastertable"){
    val masterTablePropertiesFile = "/master_table_version.properties"
    val masterTableProperties = new MasterTableProperties

    val diagnosisMasterTable = "/Diagnosis.csv"
    val readData = Source.fromInputStream(getClass.getResourceAsStream(diagnosisMasterTable))
    val filteredLines = readData.getLines().filter(!_.startsWith("#")).filter(!_.isEmpty)
    val splittedLine = filteredLines.map(line=>line.split("\\|", -1))

    val firstLine = readData.getLines().take(1).mkString
    println(firstLine)
    println(firstLine.split("\\|", -1).length)
  }

  test("testing master table by creating hashmap"){
    val masterTableDiagnosisGroupers = new MasterTableDiagnosisGroupers
    val temp = masterTableDiagnosisGroupers.readPropertiesToMap("/Diagnosis.csv")
    println(masterTableDiagnosisGroupers.getDiagCodeToDiagGrouperId())
  }

  test("1 testing masterTable on Medical Table"){
    val sparkContext = sparkSession.sparkContext
    var medicalDiags = medicalDataCreation

    val masterTableDiagnosisGroupers = new MasterTableDiagnosisGroupers
    val temp = masterTableDiagnosisGroupers.readPropertiesToMap("/Diagnosis.csv")
    val broadCastedDiagMT = sparkContext.broadcast(masterTableDiagnosisGroupers)
    val diagnosisMasterTableUDFs = new DiagnosisMasterTableUDFs(broadCastedDiagMT)
    for(i <- 1 to 4) {
      medicalDiags = medicalDiags.withColumn("diag"+i+"_grouper_id", diagnosisMasterTableUDFs.grouperId(medicalDiags("svc_diag_"+i+"_code")))
        .withColumn("diag"+i+"_grouper_desc", diagnosisMasterTableUDFs.grouperIdDesc(medicalDiags("svc_diag_"+i+"_code")))
        .withColumn("diag"+i+"_supergrouper_id", diagnosisMasterTableUDFs.superGrouperId(medicalDiags("svc_diag_"+i+"_code")))
        .withColumn("diag"+i+"_supergrouper_desc", diagnosisMasterTableUDFs.superGrouperIdDesc(medicalDiags("svc_diag_"+i+"_code")))
    }
    medicalDiags.show
    sparkContext.stop()
  }

  test("2 testing master table by creating dataframes"){
    val sparkContext = sparkSession.sparkContext
    val sqlContext = sparkSession.sqlContext

    val generateSchemas = new GenerateSchemas
    val masterTableSchema = generateSchemas.dynamicSchema("/diagnosisLayout.csv")
    val generateDataFrame = new GenerateDataFrame

    val masterTableDiagRdd = sparkContext.textFile("/home/anahcolus/IdeaProjects/scalaTest/src/main/resources/Diagnosis.csv")
    val masterTableDataFrame = generateDataFrame.createMasterDataFrame(sqlContext, masterTableDiagRdd, masterTableSchema)

    var medicalDiags = medicalDataCreation
    val broadCastedDiagMT = sparkContext.broadcast(masterTableDataFrame)
    val masterTableUdfs = new MasterTableUdfs(broadCastedDiagMT)
    for(i <- 1 to 4) {
      medicalDiags = medicalDiags.withColumn("diag"+i+"_grouper_id", masterTableUdfs.getDiagGrouperId(medicalDiags("svc_diag_"+i+"_code")))
        .withColumn("diag"+i+"_grouper_desc", masterTableUdfs.getDiagGrouperIdDesc(medicalDiags("svc_diag_"+i+"_code")))
        .withColumn("diag"+i+"_supergrouper_id", masterTableUdfs.getSuperDiagGrouperId(medicalDiags("svc_diag_"+i+"_code")))
        .withColumn("diag"+i+"_supergrouper_desc", masterTableUdfs.getsuperDiagGrouperIdDesc(medicalDiags("svc_diag_"+i+"_code")))
    }
    medicalDiags.show
    sparkContext.stop()
  }

  test("3 combining 1 hashmap and 2 dataframe"){
    val sparkContext = sparkSession.sparkContext
    val sqlContext = sparkSession.sqlContext

    val generateSchemas = new GenerateSchemas
    val masterTableSchema = generateSchemas.dynamicSchema("/diagnosisLayout.csv")
    val generateDataFrame = new GenerateDataFrame

    val masterTableDiagRdd = sparkContext.textFile("/home/anahcolus/IdeaProjects/scalaTest/src/main/resources/Diagnosis.csv")
    val masterTableDataFrame = generateDataFrame.createMasterDataFrame(sqlContext, masterTableDiagRdd, masterTableSchema)

    var medicalDiags = medicalDataCreation
    val masterTableDiagnosisGroupers = new MasterTableDiagnosisGroupers
    val temp = masterTableDiagnosisGroupers.readPropertiesToMap(masterTableDataFrame)
    val broadCastedDiagMT = sparkContext.broadcast(masterTableDiagnosisGroupers)
    val diagnosisMasterTableUDFs = new DiagnosisMasterTableUDFs(broadCastedDiagMT)
    for(i <- 1 to 4) {
      medicalDiags = medicalDiags.withColumn("diag"+i+"_grouper_id", diagnosisMasterTableUDFs.grouperId(medicalDiags("svc_diag_"+i+"_code")))
        .withColumn("diag"+i+"_grouper_desc", diagnosisMasterTableUDFs.grouperIdDesc(medicalDiags("svc_diag_"+i+"_code")))
        .withColumn("diag"+i+"_supergrouper_id", diagnosisMasterTableUDFs.superGrouperId(medicalDiags("svc_diag_"+i+"_code")))
        .withColumn("diag"+i+"_supergrouper_desc", diagnosisMasterTableUDFs.superGrouperIdDesc(medicalDiags("svc_diag_"+i+"_code")))
    }
    medicalDiags.show
    sparkContext.stop()
  }

  def medicalDataCreation : DataFrame = {
    val sQLContext = sparkSession.sqlContext
    import sQLContext.implicits._
    val medicalSequenceData = Seq(("E944.3", "", "M10.139", "M46.32",
      "diag1_grouper_id", "diag1_grouper_desc", "diag1_supergrouper_id", "diag1_supergrouper_desc",
      "diag2_grouper_id", "diag2_grouper_desc", "diag2_supergrouper_id", "diag2_supergrouper_desc",
      "diag3_grouper_id", "diag3_grouper_desc", "diag3_supergrouper_id", "diag3_supergrouper_desc",
      "diag4_grouper_id", "diag4_grouper_desc", "diag4_supergrouper_id", "diag4_supergrouper_desc"),
      ("M46.32", "820.32", "E13.341", "M10.139",
        "diag1_grouper_id", "diag1_grouper_desc", "diag1_supergrouper_id", "diag1_supergrouper_desc",
        "diag2_grouper_id", "diag2_grouper_desc", "diag2_supergrouper_id", "diag2_supergrouper_desc",
        "diag3_grouper_id", "diag3_grouper_desc", "diag3_supergrouper_id", "diag3_supergrouper_desc",
        "diag4_grouper_id", "diag4_grouper_desc", "diag4_supergrouper_id", "diag4_supergrouper_desc"),
      ("M10.139", "M22.90", "820.32", "",
        "diag1_grouper_id", "diag1_grouper_desc", "diag1_supergrouper_id", "diag1_supergrouper_desc",
        "diag2_grouper_id", "diag2_grouper_desc", "diag2_supergrouper_id", "diag2_supergrouper_desc",
        "diag3_grouper_id", "diag3_grouper_desc", "diag3_supergrouper_id", "diag3_supergrouper_desc",
        "diag4_grouper_id", "diag4_grouper_desc", "diag4_supergrouper_id", "diag4_supergrouper_desc"),
      ("E944.3", "M22.90", "E13.341", "D21.0",
        "diag1_grouper_id", "diag1_grouper_desc", "diag1_supergrouper_id", "diag1_supergrouper_desc",
        "diag2_grouper_id", "diag2_grouper_desc", "diag2_supergrouper_id", "diag2_supergrouper_desc",
        "diag3_grouper_id", "diag3_grouper_desc", "diag3_supergrouper_id", "diag3_supergrouper_desc",
        "diag4_grouper_id", "diag4_grouper_desc", "diag4_supergrouper_id", "diag4_supergrouper_desc"))
      .toDF("svc_diag_1_code", "svc_diag_2_code", "svc_diag_3_code", "svc_diag_4_code",
        "diag1_grouper_id", "diag1_grouper_desc", "diag1_supergrouper_id", "diag1_supergrouper_desc",
        "diag2_grouper_id", "diag2_grouper_desc", "diag2_supergrouper_id", "diag2_supergrouper_desc",
        "diag3_grouper_id", "diag3_grouper_desc", "diag3_supergrouper_id", "diag3_supergrouper_desc",
        "diag4_grouper_id", "diag4_grouper_desc", "diag4_supergrouper_id", "diag4_supergrouper_desc")
    medicalSequenceData
  }

}
