package officework.doingWithClasses.framework.validation

import officework.doingWithClasses.framework.emvalidation.MemberValidator
import officework.doingWithClasses.framework.goldenrules.GoldenRules
import officework.doingWithClasses.framework.utils.{ClientCfgParameters, JobCfgParameters, OutputSavingFormatUtils}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * Created by anahcolus on 4/20/17.
  */
class ValidationEligibilityAssembly(dataFrame: DataFrame, clientConfig: ClientCfgParameters, jobConfig: JobCfgParameters, sQLContext: SQLContext) {
  //applying golden rules
  val goldenRules = new GoldenRules(clientConfig.getEOC, clientConfig.getClientType)
  var eligDataFrame = goldenRules.applyEligibilityGoldenRules(dataFrame)

  val tempMemberIdTable = dataFrame.select("dw_member_id").distinct().withColumnRenamed("dw_member_id", "dw_member_id_1")
  tempMemberIdTable.createOrReplaceTempView("tempMemberIdTable")
  val memberIdDataFrame = sQLContext.sql("select row_number() over (order by dw_member_id_1) as integer_member_id,dw_member_id_1 from tempMemberIdTable")

}

object ValidationEligibilityAssembly {

  def apply(dataFrame: DataFrame, clientConfig: ClientCfgParameters, jobConfig: JobCfgParameters, sparkContext: SparkContext, sQLContext: SQLContext) : DataFrame = {
    val validationEligAssembly = new ValidationEligibilityAssembly(dataFrame, clientConfig, jobConfig, sQLContext)
    MemberValidator.setMemberValidatorDataFrame(validationEligAssembly.memberIdDataFrame, sparkContext)
    //      val memberIntRDD = memberIdDataFrame.rdd.map(row => row.toString().replace("[","").replace("]",""))
    //    OutputSavingFormatUtils.sequenceTupleFormats(memberIntRDD, eligJobConfig.getIntMemberId, ",")
    //   OutputSavingFormatUtils.textCSVFormats(memberIntRDD, eligJobConfig.getIntMemberId)
    OutputSavingFormatUtils.dataFrameToCSVFormat(validationEligAssembly.memberIdDataFrame, jobConfig.getIntMemberId)

    validationEligAssembly.eligDataFrame
  }
}