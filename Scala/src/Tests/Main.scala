package src.Tests

/**
  * Created by ramaharjan on 1/18/17.
  */
import java.io.{ByteArrayOutputStream, ObjectOutputStream}
import java.util

import Scala.src.Tests.GoldenRules
import cascading.tuple.hadoop.TupleSerializationProps
import cascading.tuple.{Tuple, Tuples}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer


object Main {
  var eoc : String = "2016-12-31"
  var clientType : String = "Medicaid"
  def main(args: Array[String]) {
    val clientConfigFile = "/home/ramaharjan/Documents/testProjects/gitHubScala/scalaTest/data/client_config.properties"
    val jobConfigFile = "/home/ramaharjan/Documents/testProjects/gitHubScala/scalaTest/data/validation_eligibility.jobcfg"

    val clientConfigProps = LoadProperties.readPropertiesToMap(clientConfigFile)
    val jobConfigProps = LoadProperties.readPropertiesToMap(jobConfigFile)

    val sourceFile = jobConfigProps("inputFile")
    val outputFile = jobConfigProps("outputDirectory")
    val sourceLayoutFile = jobConfigProps("layoutFile")

    clientType = clientConfigProps("clientType")
    eoc = clientConfigProps("cycleEndDate")

    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val layoutDataRdd = sc.textFile(sourceLayoutFile)

    val schema = dynamicSchema(sc, sourceLayoutFile)
    sc.hadoopConfiguration.set("textinputformat.record.delimiter", "^*~")

    val sourceDataRdd = sc.textFile(sourceFile)

    val eligibilityTable = eligDataFrame(sc, sqlContext, sourceDataRdd, schema)

    GoldenRules.eoc = eoc
    GoldenRules.clientType = clientType

    val dobChanged =eligibilityTable.withColumn("mbr_dob", GoldenRules.eligGoldenRuleDOB(eligibilityTable("mbr_dob"),eligibilityTable("mbr_relationship_class")))
    val relationshipCodeChanged = dobChanged.withColumn("mbr_relationship_code", GoldenRules.eligGoldenRuleRelationshipCode(dobChanged("mbr_dob")))
    relationshipCodeChanged.withColumn("mbr_relationship_desc", GoldenRules.eligGoldenRuleRelationshipDesc(relationshipCodeChanged("mbr_relationship_code")))
      .withColumn("mbr_relationship_class", GoldenRules.eligGoldenRuleRelationshipDesc(relationshipCodeChanged("mbr_relationship_code")))
      .withColumn("mbr_gender", GoldenRules.eligGoldenRuleGender(relationshipCodeChanged("mbr_gender")))
      .withColumn("ins_med_eff_date", GoldenRules.eligGoldenRuleDates(relationshipCodeChanged("ins_med_eff_date")))
      .withColumn("ins_med_term_date", GoldenRules.eligGoldenRuleDates(relationshipCodeChanged("ins_med_term_date"))).show

    val eligRDD = relationshipCodeChanged.rdd

    val eligibilityOutput = eligRDD
        .map(row => row.toString().split(","))
          .map(v => (Tuple.NULL, new Tuple(serialise(v))))
      .saveAsNewAPIHadoopFile(outputFile, classOf[Tuple], classOf[Tuple], classOf[SequenceFileOutputFormat[Tuple, Tuple]], getHadoopConf)


/*val intRDD = rdd.map(tuple => tuple.getString(2))
  .distinct()
  .zipWithUniqueId()
  .map(kv => (Tuple.NULL, new Tuple(kv._1.toString, kv._2.toString)))
  .saveAsNewAPIHadoopFile(absoluteOutputPathIntMem, classOf[Tuple], classOf[Tuple], classOf[SequenceFileOutputFormat[Tuple, Tuple]], hadoopConf)
*/
    sc.stop()
  }

  def serialise(value: Any): Array[Byte] = {
    val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(stream)
    oos.writeObject(value)
    oos.close
    stream.toByteArray
  }

  def getHadoopConf(): Configuration = {
    val hadoopConf = new Configuration();
    hadoopConf.set(TupleSerializationProps.HADOOP_IO_SERIALIZATIONS, "cascading.tuple.hadoop.TupleSerialization,org.apache.hadoop.io.serializer.WritableSerialization")
    hadoopConf.set("textinputformat.record.delimiter", "^*~")
    hadoopConf
  }

  def eligDataFrame(sc : SparkContext, sqlContext : SQLContext, inputLines : RDD[String], schema : StructType): DataFrame ={
    val rowFieldsTest = inputLines.map{line => line.split("\\^%~")}
    val rowFields = inputLines.map{line => line.split("\\^%~", -1)}.map{ array => Row.fromSeq(array.zip(schema.toSeq).map{ case (value, struct) => convertTypes(value, struct) })}

    val sqlContext = new SQLContext(sc)
    val df = sqlContext.createDataFrame(rowFields, schema)

//    df.show()
    df
  }

  def dynamicSchema(sc : SparkContext, file : String): StructType ={
    val readData = sc.textFile(file).filter(!_.startsWith("#"))
    val schema = readData.map(x=>x.split(";", -1)).map {value => StructField(value(1), dataType(value(4)))}
    val structType = StructType(schema.collect().toSeq)
    //    println(structType.prettyJson)
    structType
  }

  def dataType(dataType : String) : DataType ={
    if(dataType.equalsIgnoreCase("int")){
      IntegerType
    }/*
    else if(dataType.equalsIgnoreCase("date")){
      DateType
    }*/
    else if(dataType.equalsIgnoreCase("float")){
      FloatType
    }
    else if(dataType.equalsIgnoreCase("double")){
      DoubleType
    }
    else{
      StringType
    }
  }

  def convertTypes(value: String, struct: StructField): Any = struct.dataType match {
    case DoubleType => if(!value.isEmpty && formatValidator(value, struct)) value.toDouble else null
    case FloatType => if(!value.isEmpty && formatValidator(value, struct)) value.toFloat else null
    case DateType => value
    case IntegerType => if(!value.isEmpty && formatValidator(value, struct)) value.toInt else null
    case LongType => if(!value.isEmpty && formatValidator(value, struct)) value.toLong else null
    case _ => if(!value.isEmpty && formatValidator(value, struct)) value else null
  }

  def formatValidator(value : String, struct: StructField) : Boolean = {
    if (value == null || StringType.equals(struct.dataType)) {
      //do nothing
    } else if (IntegerType.equals(struct.dataType)) {
      if (!value.toString.matches(Patterns.INT_PATTERN)) {
        throw new RuntimeException("Invalid format for field: " + struct.name + " Type: " + struct.dataType + " Value: " + value)
      }
    } else if (DateType.equals(struct.dataType)) {
      if (!value.toString.matches(Patterns.DATE_PATTERN)) {
        throw new RuntimeException("Invalid format for field: " + struct.name + " Type: " + struct.dataType + " Value: " + value)
      }
    } else if (DoubleType.equals(struct.dataType)) {
      if (!value.toString.matches(Patterns.FLOAT_PATTERN)) {
        throw new RuntimeException("Invalid format for field: " + struct.name + " Type: " + struct.dataType + " Value: " + value)
      }
    } else if (LongType.equals(struct.dataType)) {
      if (!value.toString.matches(Patterns.INT_PATTERN)) {
        throw new RuntimeException("Invalid format for field: " + struct.name + " Type: " + struct.dataType + " Value: " + value)
      }
    } else if (FloatType.equals(struct.dataType)) {
      if (!value.toString.matches(Patterns.FLOAT_PATTERN)) {
        throw new RuntimeException("Invalid format for field: " + struct.name + " Type: " + struct.dataType + " Value: " + value)
      }
    }
    true
  }
}
