package main.scala.officework.doingWithClasses

/**
  * Created by ramaharjan on 2/1/17.
  */
class JobCfgParameters(jobCfgFile: String) {

  //loading the properties to map
  val loadProperties = new LoadProperties
  val jobConfigProps = loadProperties.readPropertiesToMap(jobCfgFile)

  def getSourceFilePath(): String ={
    jobConfigProps("inputFile")
  }

  def getSinkFilePath(): String ={
    jobConfigProps("outputDirectory")
  }

  def getIntMemberId(): String = {
    jobConfigProps("outputIntMemberId")
  }

  def getInputLayoutFilePath(): String ={
    jobConfigProps("inputLayoutFile")
  }
}
