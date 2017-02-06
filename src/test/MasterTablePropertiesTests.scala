package test

import main.scala.officework.doingWithClasses._
import main.scala.officework.doingWithClasses.masterTableUsingDF.{DiagnosisMasterTableUDFs, MasterTableDiagnosisGroupers}
import org.apache.spark.sql.SparkSession
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

  test("testing MasterTableDiagnosisGroupers"){
    val masterTableDiagnosisGroupers = new MasterTableDiagnosisGroupers
    val temp = masterTableDiagnosisGroupers.readPropertiesToMap("/Diagnosis.csv")
    println(masterTableDiagnosisGroupers.getDiagCodeToDiagGrouperId())
  }

  test("testing masterTable on Medical Table"){
    val sparkContext = sparkSession.sparkContext
    val sqlContext = sparkSession.sqlContext
    import sqlContext.implicits._
    var medicalDiags = Seq(("E944.3", "", "M10.139", "M46.32",
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

    val masterTableDiagnosisGroupers = new MasterTableDiagnosisGroupers
    val temp = masterTableDiagnosisGroupers.readPropertiesToMap("/Diagnosis.csv")
//    println(masterTableDiagnosisGroupers.getDiagCodeToDiagGrouperId())
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

  test("testing master table by creating dataframes"){
    val sparkContext = sparkSession.sparkContext
    val sqlContext = sparkSession.sqlContext

    val generateSchemas = new GenerateSchemas
    val masterTableSchema = generateSchemas.dynamicSchema("/diagnosisLayout.csv")
    val generateDataFrame = new GenerateDataFrame

    val masterTableDiagRdd = sparkContext.textFile("/home/ramaharjan/Documents/testProjects/gitHubScala/scalaTest/src/main/resources/Diagnosis.csv")
    val masterTableDataFrame = generateDataFrame.createMasterDataFrame(sqlContext, masterTableDiagRdd, masterTableSchema)

    import sqlContext.implicits._
    var medicalDiags = Seq(("E944.3", "", "M10.139", "M46.32",
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

}
