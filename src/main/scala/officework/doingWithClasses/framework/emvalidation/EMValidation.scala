package officework.doingWithClasses.framework.emvalidation

import officework.doingWithClasses.framework.dataframeutils.{GenerateDataFrame, GenerateSchemas}
import officework.doingWithClasses.framework.emvalidation.medical.{EMMedicalAssembly, ProcedureFunction}
import officework.doingWithClasses.framework.goldenrules.GoldenRules
import officework.doingWithClasses.framework.interfaces.JobInterface
import officework.doingWithClasses.framework.utils.{ClientCfgParameters, JobCfgParameters, OutputSavingFormatUtils}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}

/**
  * Created by ramaharjan on 4/20/17.
  */
class EMValidation extends JobInterface{

  override def process(clientConfig : ClientCfgParameters,
                       jobConfigFile : String,
                       recordType : String,
                       sparkContext: SparkContext,
                       sQLContext: SQLContext
                      )
  : Unit =
  {
    val jobConfig = new JobCfgParameters("/" + jobConfigFile)
    val generateSchemas = new GenerateSchemas
    val schema = generateSchemas.dynamicSchema(jobConfig.getInputLayoutFilePath)

    //defining line delimiter for source files
    val dataRdd = sparkContext.textFile(jobConfig.getSourceFilePath)

    //dataframe creating instance
    val generateDataFrame = new GenerateDataFrame
    //data frame generation for input source
    var dataFrame = generateDataFrame.createDataFrame(sQLContext, dataRdd, schema, "\\^%~")
    dataFrame = dataFrame.join(MemberValidator.getBroadCastedMemberDataFrame().value, dataFrame("dw_member_id") === MemberValidator.getBroadCastedMemberDataFrame().value("dw_member_id_1"), "inner")

    //applying golden rules
    val goldenRules = new GoldenRules(clientConfig.getEOC, clientConfig.getClientType)
    dataFrame = applyGoldenRules(recordType, dataFrame, goldenRules, sparkContext)

    dataFrame.show()
//    val outputRdd = dataFrame.rdd.map(row => row.toString().replace("[","").replace("]","")) // taking time
//    OutputSavingFormatUtils.sequenceTupleFormats(outputRdd, jobConfig.getSinkFilePath, ",")
//    OutputSavingFormatUtils.textCSVFormats(outputRdd, jobConfig.getSinkFilePath)
    OutputSavingFormatUtils.dataFrameToCSVFormat(dataFrame, jobConfig.getSinkFilePath)
  }

  def applyGoldenRules(recordType: String, dataFrame: DataFrame, goldenRules: GoldenRules, sparkContext: SparkContext) : DataFrame = recordType.toLowerCase() match {
      case "medical" => EMMedicalAssembly(dataFrame, goldenRules, sparkContext)
      case "pharmacy" => goldenRules.applyPharmacyGoldenRules(dataFrame)
      case "hra" => goldenRules.applyHRAGoldenRules(dataFrame)
      case "biometric" => goldenRules.applyBiometricGoldenRules(dataFrame)
      case "outcome" => goldenRules.applyOutcomeGoldenRules(dataFrame)
      case _ => dataFrame
  }
}
