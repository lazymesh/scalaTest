package main.scala.officework


/**
  * Created by ramaharjan on 1/30/17.
  */
class StaticValues extends scala.Serializable{

  var eoc : String = _
  var clientType : String = _

  def setEOC(endCycleDate : String) : Unit = {
    this.eoc = endCycleDate
  }

  def getEOC(): String = {
    this.eoc
  }

  def setClienType(clientType : String): Unit = {
    this.clientType = clientType
  }

  def getClientType(): String = {
    this.clientType
  }
}
