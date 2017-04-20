package officework.doingWithClasses.framework.validation

import officework.doingWithClasses.framework.dataframeutils.{GenerateDataFrame, GenerateSchemas}
import officework.doingWithClasses.framework.emvalidation.MemberValidator
import officework.doingWithClasses.framework.goldenrules._
import officework.doingWithClasses.framework.interfaces.JobInterface
import officework.doingWithClasses.framework.utils.{ClientCfgParameters, JobCfgParameters, OutputSavingFormatUtils}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SparkSession}

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
    var dataFrame = generateDataFrame.createDataFrame(sQLContext, dataRdd, schema, "\\^%~")

    if(recordType.equalsIgnoreCase("eligibility")){
      //applying golden rules
      val goldenRules = new GoldenRules(clientConfig.getEOC, clientConfig.getClientType)
      dataFrame = goldenRules.applyEligibilityGoldenRules(dataFrame)

      val tempMemberIdTable = dataFrame.select("dw_member_id").distinct().withColumnRenamed("dw_member_id", "dw_member_id_1")
      tempMemberIdTable.createOrReplaceTempView("tempMemberIdTable")
      val memberIdDataFrame = sQLContext.sql("select row_number() over (order by dw_member_id_1) as integer_member_id,dw_member_id_1 from tempMemberIdTable")

      MemberValidator.setMemberValidatorDataFrame(memberIdDataFrame, sparkContext)
//      val memberIntRDD = memberIdDataFrame.rdd.map(row => row.toString().replace("[","").replace("]",""))
      //    OutputSavingFormatUtils.sequenceTupleFormats(memberIntRDD, eligJobConfig.getIntMemberId, ",")
      //   OutputSavingFormatUtils.textCSVFormats(memberIntRDD, eligJobConfig.getIntMemberId)
      OutputSavingFormatUtils.dataFrameToCSVFormat(memberIdDataFrame, jobConfig.getIntMemberId)
    }
    val outputRdd = dataFrame.rdd.map(row => row.toString().replace("[","").replace("]",""))
//    OutputSavingFormatUtils.sequenceTupleFormats(outputRdd, jobConfig.getSinkFilePath, ",")
//    OutputSavingFormatUtils.textCSVFormats(outputRdd, jobConfig.getSinkFilePath)
    OutputSavingFormatUtils.dataFrameToCSVFormat(dataFrame, jobConfig.getSinkFilePath)
  }
}

