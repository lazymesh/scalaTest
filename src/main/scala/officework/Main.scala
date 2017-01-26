package officework

/**
  * Created by ramaharjan on 1/18/17.
  */
import cascading.tuple.{Tuple, Tuples}
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat
import org.apache.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConverters._


object Main {
  //global variables
  var eoc : String = ""
  var clientType : String = ""

  def main(args: Array[String]) {

    val clientId = args(0)+"/"
    //reading clientConfig
    val clientConfigFile = "/home/ramaharjan/Documents/testProjects/gitHubScala/scalaTest/data/client_config.properties"
    //reading jobconfig for input output recordtypes etc
    val jobConfigFile = "/home/ramaharjan/Documents/testProjects/gitHubScala/scalaTest/data/validation_eligibility.jobcfg"

    //loading the properties to map
    val clientConfigProps = LoadProperties.readPropertiesToMap(clientConfigFile)
    val jobConfigProps = LoadProperties.readPropertiesToMap(jobConfigFile)

    //defining variables
    val sourceFile = clientId + jobConfigProps("inputFile")
    val outputFile = clientId + jobConfigProps("outputDirectory")
    val outputFileIntMemberId = clientId + jobConfigProps("outputIntMemberId")
    val sourceLayoutFile = clientId + jobConfigProps("layoutFile")

    clientType = clientConfigProps("clientType")
    eoc = clientConfigProps("cycleEndDate")

    //spark configurations
    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    //schema generation for the input source
    val schema = DataFrames.dynamicSchema(sourceLayoutFile)

    //defining line delimiter for source files
    sc.hadoopConfiguration.set("textinputformat.record.delimiter", "^*~")
    val sourceDataRdd = sc.textFile(sourceFile)
    //data frame generation for input source
    val eligibilityTable = DataFrames.eligDataFrame(sqlContext, sourceDataRdd, schema)

    GoldenRules.eoc = eoc
    GoldenRules.clientType = clientType

    //applying golden rules
    //todo find efficient way for applying the rules
    val dobChanged =eligibilityTable.withColumn("mbr_dob", GoldenRules.eligGoldenRuleDOB(eligibilityTable("mbr_dob"),eligibilityTable("mbr_relationship_class")))
    val relationshipCodeChanged = dobChanged.withColumn("mbr_relationship_code", GoldenRules.eligGoldenRuleRelationshipCode(dobChanged("mbr_dob")))
    relationshipCodeChanged.withColumn("mbr_relationship_desc", GoldenRules.eligGoldenRuleRelationshipDesc(relationshipCodeChanged("mbr_relationship_code")))
      .withColumn("mbr_relationship_class", GoldenRules.eligGoldenRuleRelationshipDesc(relationshipCodeChanged("mbr_relationship_code")))
      .withColumn("mbr_gender", GoldenRules.eligGoldenRuleGender(relationshipCodeChanged("mbr_gender")))
      .withColumn("ins_med_eff_date", GoldenRules.eligGoldenRuleDates(relationshipCodeChanged("ins_med_eff_date")))
      .withColumn("ins_med_term_date", GoldenRules.eligGoldenRuleDates(relationshipCodeChanged("ins_med_term_date")))

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
      .map(kv => (Tuple.NULL, new Tuple(kv._1.toString, kv._2.toString)))
      .saveAsNewAPIHadoopFile(outputFileIntMemberId, classOf[Tuple], classOf[Tuple], classOf[SequenceFileOutputFormat[Tuple, Tuple]], ScalaUtils.getHadoopConf)

    //stopping sparkContext
    sc.stop()
  }
}
