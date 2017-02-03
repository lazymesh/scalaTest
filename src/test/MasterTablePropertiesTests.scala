package test

import main.scala.officework.doingWithClasses.{MasterTableDiagnosisGroupers, MasterTableProperties}
import org.scalatest.{BeforeAndAfterEach, FunSuite}

import scala.io.Source

/**
  * Created by ramaharjan on 2/3/17.
  */
class MasterTablePropertiesTests extends FunSuite with BeforeAndAfterEach {

  override def beforeEach() {

  }

  override def afterEach() {

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

  test("array size test"){
    println(Array("ram","hari", "3").size)
    println(Array("ram","hari", "3").length)
    println(Array("ram","hari", "3").toList.length)
    println(Array("ram","hari", "3").toList.size)
    val testelement = List("ram","hari", "3")
    println(testelement(0))

    val sentence = "this is testing for split size"
    val splitSize = sentence.split(" ", -1)
    println(splitSize(4))
    println(splitSize.size)
  }

}
