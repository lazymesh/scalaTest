package officework.doingWithClasses.framework.utils

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

  def getRecordExists(recordType : String): String ={
    if(clientConfigProps.contains(recordType.toLowerCase()+"Exists")){
      clientConfigProps(recordType.toLowerCase()+"Exists")
    }
    else {
      "true"
    }
  }
}
