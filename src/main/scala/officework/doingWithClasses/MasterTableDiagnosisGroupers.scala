package main.scala.officework.doingWithClasses

import scala.collection.immutable.HashMap
import scala.io.Source

/**
  * Created by ramaharjan on 2/3/17.
  */
class MasterTableDiagnosisGroupers {

  private val masterTableProperties = new MasterTableProperties

  val MASTER_DIAG_VERSION_COL_INDX : Int = 12

  var diagCode_diagGrouperIdMap : HashMap[String, String] = HashMap.empty[String, String]
  var diagGrouperId_diagGrouperDescMap : HashMap[String, String] = HashMap.empty[String, String]
  var diagCode_diagSuperGrouperIdMap : HashMap[String, String] = HashMap.empty[String, String]
  var diagSuperGrouperId_diagSuperGrouperDescMap : HashMap[String, String] = HashMap.empty[String, String]


  def readPropertiesToMap(file : String): Any = {
    //todo change the line below to read from spark context
    val readData = Source.fromInputStream(getClass.getResourceAsStream(file))
    val filteredLines = readData.getLines().filter(!_.startsWith("#")).filter(!_.isEmpty)
    val firstLineSplit = filteredLines.take(1).mkString.split("\\|", -1)
    val splittedLine = filteredLines.map(line=>line.split("\\|", -1))
    val masterTableLineSize = firstLineSplit.size
    val masterTableVersion = firstLineSplit(firstLineSplit.length-1).replace("\"", "").trim

    if(validateDiagnosisMasterTable(masterTableLineSize, masterTableVersion.mkString)) {
      var diagCode: String = ""
      var diagSupGrouperId: String = ""
      var diagSupGrouperDesc: String = ""
      var diagGrouperId: String = ""
      var diagGrouperDesc: String = ""

      val diagnosisMasterTableMapped = splittedLine.map(line =>{
          diagCode = line(1).replaceAll("\"", "")
          diagSupGrouperId = line(3).replaceAll("\"", "")
          diagSupGrouperDesc = line(4).replaceAll("\"", "")
          diagGrouperId = line(5).replaceAll("\"", "")
          diagGrouperDesc = line(6).replaceAll("\"", "")
          if (!diagCode.isEmpty) {
            if (diagGrouperDesc.isEmpty) diagGrouperDesc = diagSupGrouperDesc
            diagCode_diagGrouperIdMap ++= HashMap(diagCode -> diagGrouperId)
            if(!diagGrouperId_diagGrouperDescMap.contains(diagGrouperId)) {
              diagGrouperId_diagGrouperDescMap ++= HashMap(diagGrouperId -> diagGrouperDesc)
            }
            diagCode_diagSuperGrouperIdMap ++= HashMap(diagCode -> diagSupGrouperId)
            if (!diagSuperGrouperId_diagSuperGrouperDescMap.contains(diagSupGrouperId)) {
              diagSuperGrouperId_diagSuperGrouperDescMap ++= HashMap(diagSupGrouperId -> diagSupGrouperDesc)
            }
          }
      })
      println(getDiagCodeToDiagGrouperId())
      diagnosisMasterTableMapped.foreach(println)
    }
  }

  def validateDiagnosisMasterTable(lineLength : Int, version : String): Boolean = {
    if (lineLength != MASTER_DIAG_VERSION_COL_INDX)
      throw new RuntimeException("Wrong Master Tables:> Diagnosis master table has no version column or has wrong delimiter")
    if (!masterTableProperties.getMasterTableDiagVersion().equalsIgnoreCase(version))
      throw new RuntimeException("Did not get correct versions's Diagnosis Master Table:" + " Expected Version: " + masterTableProperties.getMasterTableDiagVersion() + " but got " + version)
    true
  }

  def getDiagCodeToDiagGrouperId(): HashMap[String, String] = {
    diagCode_diagGrouperIdMap
  }

  def getDiagGrouperIdToDiagGrouperDesc(): HashMap[String, String] = {
    diagGrouperId_diagGrouperDescMap
  }

  def getDiagCodeToDiagSuperGrouperId(): HashMap[String, String] = {
    diagCode_diagSuperGrouperIdMap
  }

  def getSuperGrouperIdToSuperGrouperDesc(): HashMap[String, String] = {
    diagSuperGrouperId_diagSuperGrouperDescMap
  }
}
