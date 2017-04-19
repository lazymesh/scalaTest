package officework.doingWithClasses.framework

import main.scala.officework.doingWithClasses.{ClientCfgParameters, JobCfgParameters}
import officework.doingWithClasses.framework.interfaces.JobInterface
import org.apache.spark.sql.SparkSession

import scala.io.Source

/**
  * Created by ramaharjan on 4/19/17.
  */
object SparkEngine {


  def main(args: Array[String]) {
    //spark configurations
    val sparkSession = SparkSession.builder().appName("Simple Application")
      .master("local")
      .config("", "")
      .getOrCreate()

    //    val clientId = args(0)
    val clientId = "client"
    val clientJobFile = "/framework/"+clientId+".txt"
    val readData = Source.fromInputStream(getClass.getResourceAsStream(clientJobFile)).getLines().filter(!_.startsWith("#"))
    val jobTuples = readData.map(line => line.split("=")).map(array => {
      val jobName_RecordType = array(0).split(" ")
      (jobName_RecordType(0).trim, jobName_RecordType(1).trim, array(1).trim)
    })
    val clientConfig = new ClientCfgParameters("/client_config.properties")

    jobTuples.foreach(job => {
      if(!clientConfig.getRecordExists(job._2).equalsIgnoreCase("false")){
        println(job._1+" "+job._2+" "+job._3)
        processJob(job._1).process(clientConfig, job._3, job._2, sparkSession)
      }
    })

  }

  def processJob(name: String): JobInterface = {
    val action = Class.forName("officework.doingWithClasses.framework.validation."+name).newInstance()
    action.asInstanceOf[JobInterface]
  }
}
