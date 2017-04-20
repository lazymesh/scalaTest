package officework.doingWithClasses.framework.utils

import java.util

import cascading.tuple.{Tuple, Tuples}
import main.scala.officework.ScalaUtils
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel

import scala.collection.JavaConverters._

/**
  * Created by ramaharjan on 2/9/17.
  */
object OutputSavingFormatUtils {

  def sequenceTupleFormats(rddData : RDD[String], output : String, delimeter : String): Unit ={
    ScalaUtils.deleteResource(output)
    rddData.persist(StorageLevel.MEMORY_AND_DISK_SER).coalesce(1)
      .map(row => row.toString().split(delimeter).toList.asJava)
      .map(v => (Tuple.NULL, Tuples.create(v.asInstanceOf[java.util.List[AnyRef]])))
      .saveAsNewAPIHadoopFile(output, classOf[Tuple], classOf[Tuple], classOf[SequenceFileOutputFormat[Tuple, Tuple]], ScalaUtils.getHadoopConf)
  }

  def createList(rddString : String) : util.LinkedList[AnyRef] = {
    var list = new util.LinkedList[AnyRef]
    var pos = 1
    var end = rddString.indexOf(',', 0)
    while (end >= 0) {
      list.add(rddString.substring(pos, end))
      pos = end + 1
      end = rddString.indexOf(',', pos)
    }
    list.add(rddString.substring(pos, rddString.length-1))
    println(list)
    list
  }

  def textCSVFormats(rddData : RDD[String], output : String): Unit ={
    ScalaUtils.deleteResource(output)
    rddData.persist(StorageLevel.MEMORY_AND_DISK_SER).coalesce(1).saveAsTextFile(output)
  }

  def dataFrameToCSVFormat(dfData : DataFrame, output : String): Unit ={
    ScalaUtils.deleteResource(output)
    dfData.persist(StorageLevel.MEMORY_AND_DISK_SER).coalesce(1).write.format("com.databricks.spark.csv").save(output)
  }
}