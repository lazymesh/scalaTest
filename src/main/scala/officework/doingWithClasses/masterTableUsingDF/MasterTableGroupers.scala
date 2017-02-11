package main.scala.officework.doingWithClasses.masterTableUsingDF

import main.scala.officework.doingWithClasses.{GenerateDataFrame, GenerateSchemas, MasterTableProperties}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}

import scala.collection.immutable.HashMap
import scala.io.Source

/**
  * Created by ramaharjan on 2/3/17.
  */
class MasterTableGroupers extends scala.Serializable{

  private val masterTableProperties = new MasterTableProperties

  val MASTER_DIAG_VERSION_COL_INDX : Int = 12
  val MASTER_PROC_VERSION_COL_INDX : Int = 20

  var codeToGroupersMap : HashMap[String, Array[String]] = HashMap.empty[String, Array[String]]
  var codeToGrouperIdMap : HashMap[String, String] = HashMap.empty[String, String]
  var grouperIdToGrouperDescMap : HashMap[String, String] = HashMap.empty[String, String]
  var codeToSuperGrouperIdMap : HashMap[String, String] = HashMap.empty[String, String]
  var superGrouperIdToSuperGrouperDescMap : HashMap[String, String] = HashMap.empty[String, String]

  def diagnosisMasterTableforBC(sparkSession: SparkSession, masterTableLocation : String): Unit = {
    val sparkContext = sparkSession.sparkContext
    val sqlContext = sparkSession.sqlContext
    val generateSchemas = new GenerateSchemas
    val masterTableSchema = generateSchemas.dynamicSchema("/diagnosisLayout.csv")
    val generateDataFrame = new GenerateDataFrame
    val masterTableDiagRdd = sparkContext.textFile(masterTableLocation)
    val masterTableDF = generateDataFrame.createMasterDataFrame(sqlContext, masterTableDiagRdd, masterTableSchema)

    var diagCode: String = ""
    var diagGrouperId: String = ""
    var diagGrouperDesc: String = ""
    var diagSupGrouperId: String = ""
    var diagSupGrouperDesc: String = ""

    def getValues(row: Row, names: Seq[String]): HashMap[String, Array[String]] = {
      diagCode = row.getAs[Any](names(0)).toString
      diagGrouperId = row.getAs[Any](names(1)).toString
      diagGrouperDesc = row.getAs[Any](names(2)).toString
      diagSupGrouperId = row.getAs[Any](names(3)).toString
      diagSupGrouperDesc = row.getAs[Any](names(4)).toString

      if (diagGrouperDesc.isEmpty){ diagGrouperDesc = diagSupGrouperDesc}
      codeToGroupersMap ++= HashMap(diagCode -> Array(diagGrouperId, diagGrouperDesc, diagSupGrouperId, diagSupGrouperDesc))
      codeToGroupersMap
    }
    val names = Seq("diagnosisCode", "grouperID", "grouperDescription", "superGrouperID", "superGrouperDescription")
    import sqlContext.implicits._
    var hashMap = masterTableDF.map(getValues(_, names)).first()
    hashMap.foreach(println)
  }


  def diagnosisMasterTableToMap(masterTableLocation : String): Any = {
    //todo change the line below to read from spark context
    val readData = Source.fromFile(masterTableLocation)
    val readData2 = Source.fromFile(masterTableLocation)
    val filteredLines = readData.getLines().filter(!_.startsWith("#")).filter(!_.isEmpty)
    val copyFilteredLines = readData2.getLines().filter(!_.startsWith("#")).filter(!_.isEmpty).take(1)
    val splittedLine = filteredLines.map(line=>line.split("\\|", -1))
    val firstLineSplit = copyFilteredLines.mkString.split("\\|", -1)
    val masterTableLineSize = firstLineSplit.size
    val masterTableVersion = firstLineSplit(firstLineSplit.length-1).replace("\"", "").trim

    if(validateDiagnosisMasterTable(masterTableLineSize, masterTableVersion.mkString)) {
      var diagCode: String = ""
      var diagSupGrouperId: String = ""
      var diagSupGrouperDesc: String = ""
      var diagGrouperId: String = ""
      var diagGrouperDesc: String = ""

      var tempCodeToGroupersMap : HashMap[String, Array[String]] = HashMap.empty[String, Array[String]]

      var finaldata = splittedLine.map(line => {
          diagCode = line(1).replaceAll("\"", "")
          diagSupGrouperId = line(3).replaceAll("\"", "")
          diagSupGrouperDesc = line(4).replaceAll("\"", "")
          diagGrouperId = line(5).replaceAll("\"", "")
          diagGrouperDesc = line(6).replaceAll("\"", "")
          if (!diagCode.isEmpty) {
            if (diagGrouperDesc.isEmpty){ diagGrouperDesc = diagSupGrouperDesc}
            tempCodeToGroupersMap ++= HashMap(diagCode -> Array(diagGrouperId, diagGrouperDesc, diagSupGrouperId, diagSupGrouperDesc))
            addCodeToDiagGrouperId(diagCode, diagGrouperId)
            if(!grouperIdToGrouperDescMap.contains(diagGrouperId)) {
              addGrouperIdToDiagGrouperDesc(diagGrouperId, diagGrouperDesc)
            }
            addCodeToDiagSuperGrouperId(diagCode, diagSupGrouperId)
            if (!superGrouperIdToSuperGrouperDescMap.contains(diagSupGrouperId)) {
              addSuperGrouperIdToSuperGrouperDesc(diagSupGrouperId, diagSupGrouperDesc)
            }
          }
        tempCodeToGroupersMap
      })
      for (elem <- finaldata) {
        codeToGroupersMap ++= elem
      }
    }
  }

  def procedureMasterTableToMap(masterTableLocation : String): Any = {
    //todo change the line below to read from spark context
    val readData = Source.fromFile(masterTableLocation)
    val readData2 = Source.fromFile(masterTableLocation)
    val filteredLines = readData.getLines().filter(!_.startsWith("#")).filter(!_.isEmpty)
    val copyFilteredLines = readData2.getLines().filter(!_.startsWith("#")).filter(!_.isEmpty).take(1)
    val splittedLine = filteredLines.map(line=>line.split("\\|", -1))
    val firstLineSplit = copyFilteredLines.mkString.split("\\|", -1)
    val masterTableLineSize = firstLineSplit.size
    val masterTableVersion = firstLineSplit(firstLineSplit.length-1).replace("\"", "").trim

    if(validateProcedureMasterTable(masterTableLineSize, masterTableVersion.mkString)) {
      var procCode = ""
      var procType = ""
      var procGrouperId = ""
      var procGrouperDesc = ""
      var procSubGrouperId = ""
      var procSubGrouperDesc = ""

      splittedLine.foreach(line => {
        procCode = line(1).replaceAll("\"", "")
        procType = line(4).replaceAll("\"", "")
        procGrouperId = line(5).replaceAll("\"", "")
        procGrouperDesc = line(6).replaceAll("\"", "")
        procSubGrouperId = line(18).replaceAll("\"", "")
        procSubGrouperDesc = line(7).replaceAll("\"", "")

        if (!procCode.isEmpty) {
          val procCodeprocType = procCode + procType
          addCodeToDiagGrouperId(procCodeprocType, procGrouperId)
          addGrouperIdToDiagGrouperDesc(procCodeprocType, procGrouperDesc)
          addCodeToDiagSuperGrouperId(procCodeprocType, procSubGrouperId)
          if (procSubGrouperDesc.isEmpty) procSubGrouperDesc = procGrouperDesc
          addSuperGrouperIdToSuperGrouperDesc(procCodeprocType, procSubGrouperDesc)
        }
      })
    }
  }

  def readPropertiesToMap(sparkContext : SparkContext, sqlContext : SQLContext, masterTableLocation : String): Unit = {
    val generateSchemas = new GenerateSchemas
    val masterTableSchema = generateSchemas.dynamicSchema("/diagnosisLayout.csv")
    val generateDataFrame = new GenerateDataFrame
    val masterTableDiagRdd = sparkContext.textFile(masterTableLocation)
    val masterTableDF = generateDataFrame.createMasterDataFrame(sqlContext, masterTableDiagRdd, masterTableSchema)

    var diagCode: String = ""
    var diagGrouperId: String = ""
    var diagGrouperDesc: String = ""
    var diagSupGrouperId: String = ""
    var diagSupGrouperDesc: String = ""

    def getValues(row: Row, names: Seq[String]) = {
      diagCode = row.getAs[Any](names(0)).toString
      diagGrouperId = row.getAs[Any](names(1)).toString
      diagGrouperDesc = row.getAs[Any](names(2)).toString
      diagSupGrouperId = row.getAs[Any](names(3)).toString
      diagSupGrouperDesc = row.getAs[Any](names(4)).toString

      if (!diagCode.isEmpty) {
        if (diagGrouperDesc.isEmpty){ diagGrouperDesc = diagSupGrouperDesc}
        addCodeToDiagGrouperId(diagCode, diagGrouperId)
        if(!grouperIdToGrouperDescMap.contains(diagGrouperId)) {
          addGrouperIdToDiagGrouperDesc(diagGrouperId, diagGrouperDesc)
        }
        addCodeToDiagSuperGrouperId(diagCode, diagSupGrouperId)
        if (!superGrouperIdToSuperGrouperDescMap.contains(diagSupGrouperId)) {
          addSuperGrouperIdToSuperGrouperDesc(diagSupGrouperId, diagSupGrouperDesc)
        }
      }
    }
    val names = Seq("diagnosisCode", "grouperID", "grouperDescription", "superGrouperID", "superGrouperDescription")
    masterTableDF.rdd.collect().map(getValues(_, names))
  }

  def validateDiagnosisMasterTable(lineLength : Int, version : String): Boolean = {
    if (lineLength != MASTER_DIAG_VERSION_COL_INDX)
      throw new RuntimeException("Wrong Master Tables:> Diagnosis master table has no version column or has wrong delimiter")
    if (!masterTableProperties.getMasterTableDiagVersion().equalsIgnoreCase(version))
      throw new RuntimeException("Did not get correct versions's Diagnosis Master Table:" + " Expected Version: " + masterTableProperties.getMasterTableDiagVersion() + " but got " + version)
    true
  }

  def validateProcedureMasterTable(lineLength : Int, version : String): Boolean = {
    if (lineLength != MASTER_PROC_VERSION_COL_INDX)
      throw new RuntimeException("Wrong Master Tables:> Diagnosis master table has no version column or has wrong delimiter")
    if (!masterTableProperties.getMasterTableProcVersion().equalsIgnoreCase(version))
      throw new RuntimeException("Did not get correct versions's Diagnosis Master Table:" + " Expected Version: " + masterTableProperties.getMasterTableProcVersion() + " but got " + version)
    true
  }

  def addCodeToDiagGrouperId(diagCode:String, diagGrouperId: String): Any = {
    codeToGrouperIdMap ++= HashMap(diagCode -> diagGrouperId)
  }

  def addGrouperIdToDiagGrouperDesc(diagGrouperId: String, diagGrouperDesc:String): Any = {
    grouperIdToGrouperDescMap ++= HashMap(diagGrouperId -> diagGrouperDesc)
  }

  def addCodeToDiagSuperGrouperId(diagCode: String, diagSupGrouperId: String): Any = {
    codeToSuperGrouperIdMap ++= HashMap(diagCode -> diagSupGrouperId)
  }

  def addSuperGrouperIdToSuperGrouperDesc(diagSupGrouperId: String, diagSupGrouperDesc: String): Any = {
    superGrouperIdToSuperGrouperDescMap ++= HashMap(diagSupGrouperId -> diagSupGrouperDesc)
  }

  def getCodeToDiagGrouperId(): HashMap[String, String] = {
    codeToGrouperIdMap
  }

  def getGrouperIdToDiagGrouperDesc(): HashMap[String, String] = {
    grouperIdToGrouperDescMap
  }

  def getCodeToDiagSuperGrouperId(): HashMap[String, String] = {
    codeToSuperGrouperIdMap
  }

  def getSuperGrouperIdToSuperGrouperDesc(): HashMap[String, String] = {
    superGrouperIdToSuperGrouperDescMap
  }

  def getDiagGrouperId(diagCode : String): String = {
    codeToGroupersMap.get(diagCode).getOrElse(0, "Ungroupable").toString
  }

  def getDiagGrouperDesc(diagCode : String): HashMap[String, String] = {
    grouperIdToGrouperDescMap
  }

  def getSuperGrouperId(diagCode : String): HashMap[String, String] = {
    codeToSuperGrouperIdMap
  }

  def getSuperGrouperDesc(diagCode : String): HashMap[String, String] = {
    superGrouperIdToSuperGrouperDescMap
  }
}
