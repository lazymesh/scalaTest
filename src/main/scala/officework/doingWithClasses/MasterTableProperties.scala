package main.scala.officework.doingWithClasses

/**
  * Created by ramaharjan on 2/3/17.
  */
class MasterTableProperties extends scala.Serializable {

  val masterTablePropertiesFile = "/master_table_version.properties"
  //loading the properties to map
  val loadProperties = new LoadProperties
  val masterTableProps = loadProperties.readPropertiesToMap(masterTablePropertiesFile)

  def getMasterTableProcVersion() : String ={
    masterTableProps("master_table_proc_version")
  }

  def getMasterTableDiagVersion() : String ={
    masterTableProps("master_table_diag_version")
  }

  def getMasterTableEpisVersion() : String ={
    masterTableProps("master_table_episode_version")
  }

  def getMasterTableAspVersion() : String ={
    masterTableProps("master_table_medcre_asp_version")
  }

  def getMasterTablePhyVersion() : String ={
    masterTableProps("master_table_medcre_phy_version")
  }

  def getMasterTableZipVersion() : String ={
    masterTableProps("master_table_medcre_zip_version")
  }

  def getMegVersion() : String ={
    masterTableProps("meg_version")
  }

  def getMaraVersion() : String ={
    masterTableProps("mara_version")
  }
}
