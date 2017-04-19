package officework.doingWithClasses.framework.validation

import main.scala.officework.doingWithClasses.{ClientCfgParameters, GenerateDataFrame, GenerateSchemas, JobCfgParameters}
import officework.doingWithClasses.framework.interfaces.JobInterface
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SparkSession}

/**
  * Created by ramaharjan on 4/19/17.
  */
class ValidationJob extends JobInterface{

  override def process(clientConfig : ClientCfgParameters,
                       jobConfigFile : String,
                       recordType : String,
                       sparkSession : SparkSession)
  : Unit =
  {
    val sparkContext = sparkSession.sparkContext
    val sqlContext = sparkSession.sqlContext

    val jobConfig = new JobCfgParameters("/"+jobConfigFile)
    val generateSchemas = new GenerateSchemas
    val schema = generateSchemas.dynamicSchema(jobConfig.getInputLayoutFilePath)

    //defining line delimiter for source files
    sparkContext.hadoopConfiguration.set("textinputformat.record.delimiter", "^*~")
    val dataRdd = sparkContext.textFile(jobConfig.getSourceFilePath)

    //dataframe creating instance
    val generateDataFrame = new GenerateDataFrame
    //data frame generation for input source
    var dataFrame = generateDataFrame.createDataFrame(sqlContext, dataRdd, schema, "\\^%~")
    dataFrame.show
  }
}

