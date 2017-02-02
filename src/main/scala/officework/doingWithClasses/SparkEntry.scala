package main.scala.officework.doingWithClasses

import cascading.tuple.{Tuple, Tuples}
import main.scala.officework.ScalaUtils
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters._

/**
  * Created by ramaharjan on 2/1/17.
  */
object SparkEntry {

  def main(args: Array[String]) {

    //    val clientId = args(0)+"/"
    val eligJobConfig = new JobCfgParameters("/validation_eligibility.jobcfg")
    val medicalJobConfig = new JobCfgParameters("/emValidation_Medical.jobcfg")

    val clientConfig = new ClientCfgParameters("/client_config.properties")

    //spark configurations
    val sparkSession = SparkSession.builder().appName("Simple Application")
      .master("local")
      .config("", "")
      .getOrCreate()

    //    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val sc = sparkSession.sparkContext
    val sqlContext = sparkSession.sqlContext

    //schema generation for the input source
    val generateSchemas = new GenerateSchemas
    val eligSchema = generateSchemas.dynamicSchema(eligJobConfig.getInputLayoutFilePath)
    val medicalSchema = generateSchemas.dynamicSchema(medicalJobConfig.getInputLayoutFilePath)

    //defining line delimiter for source files
    sc.hadoopConfiguration.set("textinputformat.record.delimiter", "^*~")
    val eligibilityDataRdd = sc.textFile(eligJobConfig.getSourceFilePath)
    val medicalDataRdd = sc.textFile(medicalJobConfig.getSourceFilePath)

    //data frame generation for input source
    val generateDataFrame = new GenerateDataFrame
    val eligibilityTable = generateDataFrame.createDataFrame(sqlContext, eligibilityDataRdd, eligSchema)
    val medicalTable = generateDataFrame.createDataFrame(sqlContext, medicalDataRdd, medicalSchema)

    //applying golden rules
    val goldenRules = new GoldenRules(clientConfig.getEOC, clientConfig.getClientType)
    val eligibilityGoldenRulesApplied = goldenRules.applyEligibilityGoldenRules(eligibilityTable)

    //deleting the outputs if they exists
    ScalaUtils.deleteResource(eligJobConfig.getSinkFilePath)
    ScalaUtils.deleteResource(eligJobConfig.getIntMemberId)

    //eligibility validation output
    val eligRDD = eligibilityGoldenRulesApplied.rdd.map(row => row.toString())
    val eligibilityOutput = eligRDD
      .map(row => row.toString().split(",").toList.asJava)
      .map(v => (Tuple.NULL, Tuples.create(v.asInstanceOf[java.util.List[AnyRef]])))
      .saveAsNewAPIHadoopFile(eligJobConfig.getSinkFilePath, classOf[Tuple], classOf[Tuple], classOf[SequenceFileOutputFormat[Tuple, Tuple]], ScalaUtils.getHadoopConf)

    //integer member id output
//    val memberIdRDD = eligibilityTable.select("dw_member_id").distinct().rdd
    val memberId = eligibilityTable.select("dw_member_id").distinct().withColumnRenamed("dw_member_id", "dw_member_id_1")
    memberId.createOrReplaceTempView("simpleTable")
    val memberIdRDD = sqlContext.sql("select row_number() over (order by dw_member_id_1) as int_member_id,dw_member_id_1 from simpleTable")

    val intRDD = memberIdRDD.rdd
      .map(kv => (Tuple.NULL, new Tuple(kv(0).toString, kv(1).toString)))
      .saveAsNewAPIHadoopFile(eligJobConfig.getIntMemberId, classOf[Tuple], classOf[Tuple], classOf[SequenceFileOutputFormat[Tuple, Tuple]], ScalaUtils.getHadoopConf)

    val medicalDF = medicalTable.join(memberIdRDD, medicalTable("dw_member_id") === memberIdRDD("dw_member_id_1"), "inner")
    medicalDF.drop(medicalDF.col("dw_member_id_1"))

    val medicalGoldenRulesApplied = goldenRules.applyMedialGoldenRules(medicalDF).show

    //stopping sparkContext
    sc.stop()
  }
}
