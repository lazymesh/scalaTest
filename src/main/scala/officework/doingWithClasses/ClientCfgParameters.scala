package main.scala.officework.doingWithClasses

/**
  * Created by ramaharjan on 2/1/17.
  */
class ClientCfgParameters(clientConfigFile : String) {

  private var eoc = ""
  private var clientType = ""

  val loadProperties = new LoadProperties
  val clientConfigProps = loadProperties.readPropertiesToMap(clientConfigFile)

  def getEOC() : String = {
    eoc = clientConfigProps("cycleEndDate")
    eoc
  }

  def getClientType(): String ={
    clientType = clientConfigProps("clientType")
    clientType
  }
}
