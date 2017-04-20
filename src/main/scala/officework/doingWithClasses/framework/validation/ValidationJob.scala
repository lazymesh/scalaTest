package officework.doingWithClasses.framework.validation

import officework.doingWithClasses.framework.dataframeutils.{GenerateDataFrame, GenerateSchemas}
import officework.doingWithClasses.framework.interfaces.JobInterface
import officework.doingWithClasses.framework.utils.{ClientCfgParameters, JobCfgParameters, OutputSavingFormatUtils}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * Created by ramaharjan on 4/19/17.
  */
class ValidationJob extends JobInterface{

  override def process(clientConfig : ClientCfgParameters,
                       jobConfigFile : String,
                       recordType : String,
                       sparkContext: SparkContext,
                       sQLContext: SQLContext
                      )
  : Unit =
  {
    val jobConfig = new JobCfgParameters("/"+jobConfigFile)
    val generateSchemas = new GenerateSchemas
    val schema = generateSchemas.dynamicSchema(jobConfig.getInputLayoutFilePath)

    //defining line delimiter for source files
    sparkContext.hadoopConfiguration.set("textinputformat.record.delimiter", "^*~")
    val dataRdd = sparkContext.textFile(jobConfig.getSourceFilePath)

    //dataframe creating instance
    val generateDataFrame = new GenerateDataFrame
    //data frame generation for input source
    var dataFrame = generateDataFrame.createDataFrame(recordType, sQLContext, dataRdd, schema, "\\^%~")

    dataFrame = applyDataSpecificFunctions(recordType, dataFrame, clientConfig, jobConfig, sparkContext, sQLContext)

    val outputRdd = dataFrame.rdd.map(row => row.toString().replace("[","").replace("]",""))
//    OutputSavingFormatUtils.sequenceTupleFormats(outputRdd, jobConfig.getSinkFilePath, ",")
//    OutputSavingFormatUtils.textCSVFormats(outputRdd, jobConfig.getSinkFilePath)
    OutputSavingFormatUtils.dataFrameToCSVFormat(dataFrame, jobConfig.getSinkFilePath)
  }

  def applyDataSpecificFunctions(recordType: String, dataFrame: DataFrame, clientCfg: ClientCfgParameters, jobCfg: JobCfgParameters, sparkContext: SparkContext, sQLContext: SQLContext) : DataFrame = recordType.toLowerCase() match {
    case "eligibility" => ValidationEligibilityAssembly(dataFrame, clientCfg, jobCfg, sparkContext, sQLContext)
    case _ => dataFrame
  }
}

