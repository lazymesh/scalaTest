package test

import main.scala.officework.doingWithClasses.masterTableUsingDF.{DiagnosisMasterTableUDFs, ProcedureMasterTableUDFs}
import main.scala.officework.doingWithClasses._
import main.scala.officework.doingWithClasses.mara.MaraAssembly
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.SparkFiles
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterEach, FunSuite}

import scala.io.Source

/**
  * Created by ramaharjan on 3/7/17.
  */
class MaraTests extends FunSuite with BeforeAndAfterEach {

  val dfsWorkingDir = FileSystem.get(new Configuration()).getWorkingDirectory
  val mara1DatFile = "/mara3_9_0/MARA1.dat"
  val mara2DatFile = "/mara3_9_0/MARA2.dat"
  val maraLicenseFile = "/mara3_9_0/mara.lic"

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

  test("mara assembly test"){

    val sparkContext = sparkSession.sparkContext
    val sqlContext = sparkSession.sqlContext

    val eligibilityDataFileLocation = "/mara/Eligibility.csv"
    val medicalDataFileLocation = "/mara/Medical.csv"
    val pharmacyDataFileLocation = "/mara/Pharmacy.csv"

    val eligMaraLayout = "/mara/eligibilityLayout.csv"
    val medicalMaraLayout = "/mara/medicalLayout.csv"
    val pharmacyMaraLayout = "/mara/pharmacyLayout.csv"

    val generateSchemas = new GenerateSchemas
    val eligSchema = generateSchemas.dynamicSchema(eligMaraLayout)
    val medicalSchema = generateSchemas.dynamicSchema(medicalMaraLayout)
    val pharmacySchema = generateSchemas.dynamicSchema(pharmacyMaraLayout)

    val generateDataFrame = new GenerateDataFrame
    val eligTable = generateDataFrame.createDataFrameFromResource(sparkContext, sqlContext, eligibilityDataFileLocation, eligSchema, "\\^%~")
    val medicalTable = generateDataFrame.createDataFrameFromResource(sparkContext, sqlContext, medicalDataFileLocation, medicalSchema, "\\^%~")
    val pharmacyTable = generateDataFrame.createDataFrameFromResource(sparkContext, sqlContext, pharmacyDataFileLocation, pharmacySchema, "\\^%~")

//    sparkContext.addFile(dfsWorkingDir+mara1DatFile)
    sparkContext.addFile(dfsWorkingDir+mara2DatFile)
    sparkContext.addFile(dfsWorkingDir+maraLicenseFile)

    val maraAssembly = new MaraAssembly(eligTable, medicalTable, pharmacyTable, "2017-01-30")
    maraAssembly.maraCalculator

    sparkContext.stop

  }

}
