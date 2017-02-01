package main.scala.officework.doingWithClasses

/**
  * Created by ramaharjan on 2/1/17.
  */
class ClientCfgParameters(clientConfigFile : String) {

  val loadProperties = new LoadProperties
  val clientConfigProps = loadProperties.readPropertiesToMap(clientConfigFile)

  def getEOC() : String = {
    clientConfigProps("cycleEndDate")
  }

  def getClientType(): String ={
    clientConfigProps("clientType")
  }
}
