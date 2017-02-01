package main.scala.officework.doingWithObjects

/**
  * Created by ramaharjan on 1/18/17.
  */
import cascading.tuple.{Tuple, Tuples}
import main.scala.officework.ScalaUtils
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat
import org.apache.spark.sql._

import scala.collection.JavaConverters._


object Main {

  def main(args: Array[String]) {

//    val clientId = args(0)+"/"
    //reading clientConfig
    val clientConfigFile = "/client_config.properties"
    //reading jobconfig for input output recordtypes etc
    val jobConfigFile = "/validation_eligibility.jobcfg"

    //loading the properties to map
    val clientConfigProps = LoadProperties.readPropertiesToMap(clientConfigFile)
    val jobConfigProps = LoadProperties.readPropertiesToMap(jobConfigFile)

    //defining variables
    val sourceFile = jobConfigProps("inputFile")
    val outputFile = jobConfigProps("outputDirectory")
    val outputFileIntMemberId = jobConfigProps("outputIntMemberId")
    val sourceLayoutFile = jobConfigProps("layoutFile")

    val clientType = clientConfigProps("clientType")
    val eoc = clientConfigProps("cycleEndDate")

    //spark configurations
      val sparkSession = SparkSession.builder().appName("Simple Application")
        .master("local")
        .config("", "")
        .getOrCreate()

//    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val sc = sparkSession.sparkContext
    val sqlContext = sparkSession.sqlContext

    //schema generation for the input source
    val schema = DataFrames.dynamicSchema(sourceLayoutFile)

    //defining line delimiter for source files
    sc.hadoopConfiguration.set("textinputformat.record.delimiter", "^*~")
    val sourceDataRdd = sc.textFile(sourceFile)
    //data frame generation for input source
    val eligibilityTable = DataFrames.eligDataFrame(sqlContext, sourceDataRdd, schema)

      GoldenRules.eoc=eoc
      GoldenRules.clientType=clientType
    //applying golden rules
    //todo find efficient way for applying the rules
    val dobChanged =eligibilityTable.withColumn("mbr_dob", GoldenRules.eligGoldenRuleDOB(eligibilityTable("mbr_dob"),eligibilityTable("mbr_relationship_class")))
    val relationshipCodeChanged = dobChanged.withColumn("mbr_relationship_code", GoldenRules.eligGoldenRuleRelationshipCode(dobChanged("mbr_relationship_code"), dobChanged("mbr_dob")))
    relationshipCodeChanged.withColumn("mbr_relationship_desc", GoldenRules.eligGoldenRuleRelationshipDesc(relationshipCodeChanged("mbr_relationship_code")))
      .withColumn("mbr_relationship_class", GoldenRules.eligGoldenRuleRelationshipDesc(relationshipCodeChanged("mbr_relationship_code")))
      .withColumn("mbr_gender", GoldenRules.eligGoldenRuleGender(relationshipCodeChanged("mbr_gender")))
      .withColumn("ins_med_eff_date", GoldenRules.eligGoldenRuleDates(relationshipCodeChanged("ins_med_eff_date")))
      .withColumn("ins_med_term_date", GoldenRules.eligGoldenRuleDates(relationshipCodeChanged("ins_med_term_date"))).show

    //deleting the outputs if they exists
    ScalaUtils.deleteResource(outputFile)
    ScalaUtils.deleteResource(outputFileIntMemberId)

    //eligibility validation output
    val eligRDD = relationshipCodeChanged.rdd.map(row => row.toString())
    val eligibilityOutput = eligRDD
        .map(row => row.toString().split(",").toList.asJava)
        .map(v => (Tuple.NULL, Tuples.create(v.asInstanceOf[java.util.List[AnyRef]])))
        .saveAsNewAPIHadoopFile(outputFile, classOf[Tuple], classOf[Tuple], classOf[SequenceFileOutputFormat[Tuple, Tuple]], ScalaUtils.getHadoopConf)

    //integer member id output
    val memberIdRDD = eligibilityTable.select("dw_member_id").rdd
    val intRDD = memberIdRDD
      .distinct()
      .zipWithUniqueId()
      .map(kv => (Tuple.NULL, new Tuple(kv._1(0).toString, kv._2.toString)))
      .saveAsNewAPIHadoopFile(outputFileIntMemberId, classOf[Tuple], classOf[Tuple], classOf[SequenceFileOutputFormat[Tuple, Tuple]], ScalaUtils.getHadoopConf)

    //stopping sparkContext
    sc.stop()
  }
}
