package main.scala.officework.doingWithClasses.datasets

import main.scala.officework.doingWithClasses.JobCfgParameters
import org.apache.spark.sql.SparkSession

/**
  * Created by ramaharjan on 3/3/17.
  */
object DataSetEntry {
  def main(args: Array[String]): Unit = {

    val eligJobConfig = new JobCfgParameters("/validation_eligibility.jobcfg")
    //spark configurations
    val sparkSession = SparkSession.builder().appName("Simple Application")
      .master("local")
      .config("", "")
      .getOrCreate()

    //    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val sc = sparkSession.sparkContext
    val sqlContext = sparkSession.sqlContext

//    sc.hadoopConfiguration.set("textinputformat.record.delimiter", "^*~")
    val eligibilityDataset = sqlContext.read.text(eligJobConfig.getSourceFilePath)
    eligibilityDataset.show()

    sc.stop()
    sparkSession.stop()
  }

}
