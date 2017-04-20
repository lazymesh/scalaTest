package officework.doingWithClasses.framework.emvalidation

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.DataFrame

/**
  * Created by ramaharjan on 4/20/17.
  */
object MemberValidator extends Serializable{

  var memberIdDF : DataFrame = _

  var broadCastedmemberIdDF : Broadcast[DataFrame] = _

  def setMemberValidatorDataFrame(dataFrame: DataFrame, sparkContext: SparkContext) : Unit ={
    this.memberIdDF = dataFrame
    this.broadCastedmemberIdDF = sparkContext.broadcast(memberIdDF)
  }

  def getMemberValidatorDataFrame() : DataFrame = {
    this.memberIdDF
  }

  def getBroadCastedMemberDataFrame() : Broadcast[DataFrame] = {
    this.broadCastedmemberIdDF
  }

}
