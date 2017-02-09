package main.scala.officework.doingWithClasses

import cascading.tuple.{Tuple, Tuples}
import main.scala.officework.ScalaUtils
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

import scala.collection.JavaConverters._

/**
  * Created by ramaharjan on 2/9/17.
  */
object OutputSavingFormatUtils {

  def sequenceTupleFormats(rddData : RDD[String], output : String, delimeter : String): Unit ={
  rddData.coalesce(1)
  .map(row => row.toString().split(delimeter).toList.asJava)
  .map(v => (Tuple.NULL, Tuples.create(v.asInstanceOf[java.util.List[AnyRef]])))
  .saveAsNewAPIHadoopFile(output, classOf[Tuple], classOf[Tuple], classOf[SequenceFileOutputFormat[Tuple, Tuple]], ScalaUtils.getHadoopConf)
}

  def textCSVFormats(rddData : RDD[String], output : String): Unit ={
    rddData.coalesce(1).saveAsTextFile(output)
  }

  def dataFrameToCSVFormat(dfData : DataFrame, output : String): Unit ={
    dfData.coalesce(1).write.format("com.databricks.spark.csv").option("header", true).save(output)
  }
}