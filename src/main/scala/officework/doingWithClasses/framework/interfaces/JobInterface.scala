package officework.doingWithClasses.framework.interfaces

import officework.doingWithClasses.framework.utils.ClientCfgParameters
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SparkSession}


/**
  * Created by ramaharjan on 4/19/17.
  */
abstract class JobInterface {

  def process(
               clientConfig : ClientCfgParameters,
               jobConfig : String,
               recordType : String,
               sparkContext: SparkContext,
               sQLContext: SQLContext
             )
}
