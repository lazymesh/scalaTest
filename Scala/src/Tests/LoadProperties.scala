package src.Tests

import scala.io.Source

/**
  * Created by ramaharjan on 1/23/17.
  */
object LoadProperties {

  def readPropertiesToMap(file : String): Map[String, String] ={

    val readData = Source.fromFile(file)
    val filteredLines = readData.getLines().filter(!_.startsWith("#")).filter(!_.isEmpty)
    val mapData = filteredLines.map(line=>line.split("=")).map(kv => (kv(0).trim -> kv(1).trim)).toMap
    mapData
  }
}
