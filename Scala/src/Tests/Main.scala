package src.Tests

/**
  * Created by ramaharjan on 1/18/17.
  */
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions.udf

object Main {
  def main(args: Array[String]) {
    val sourceFile = "file:/home/ramaharjan/bugs/110054/elig.csv"
    val layoutFile = "file:/home/ramaharjan/bugs/110054/eligibilityLayout.csv"
    val eligibilityGoldenRule = "file:/home/ramaharjan/Documents/testProjects/gitHubScala/scalaTest/data/eligibilityGolderRule.csv"
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val goldenRuleRdd = sc.textFile(eligibilityGoldenRule)

    val layoutDataRdd = sc.textFile(layoutFile)
    val schema = dynamicSchema(sc, layoutFile)

    sc.hadoopConfiguration.set("textinputformat.record.delimiter", "^*~")
    val sourceDataRdd = sc.textFile(sourceFile)

//    case class eligibility(schema : StructType)

    val eligibilityTable = dataFrame(sc, sqlContext, sourceDataRdd, schema)

    val goldenRuleDataSet = goldenRuleDS(sqlContext, goldenRuleRdd)

    eligibilityTable.withColumn("mbr_dob", udfScoreToCategory(eligibilityTable("mbr_dob"), goldenRuleDataSet("field", "firstValue", "secondValue"))).show

    eligibilityTable.collect().foreach(println)

    sc.stop()
  }

  def udfScoreToCategory = udf((score: Column, goldenRule: Column) => {score match {
    case t if goldenRule.getField("field") >= 80 => "A"
    case t if t >= 60 => "B"
    case t if t >= 35 => "C"
    case _ => "D"
  }})

  case class GoldenRule(field : String, firstValue : String, SecondValue : String)
  def goldenRuleDS(sQLContext: SQLContext, goldenRuleLines : RDD[String]) : DataFrame = {
    val goldenRules = goldenRuleLines.map(line=>line.split(";", -1)).map {v => GoldenRule(v(0), v(1), v(2))}
    import sQLContext.implicits._
    goldenRules.toDF()
  }


  def dataFrame(sc : SparkContext, sqlContext : SQLContext, inputLines : RDD[String], schema : StructType): DataFrame ={
    val rowFieldsTest = inputLines.map{line => line.split("\\^%~")}
    val rowFields = inputLines.map{line => line.split("\\^%~", -1)}.map{ array => Row.fromSeq(array.zip(schema.toSeq).map{ case (value, struct) => convertTypes(value, struct) })}

    val sqlContext = new SQLContext(sc)
    val df = sqlContext.createDataFrame(rowFields, schema)

    df.show()
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
    } else if ("float".equals(struct.dataType)) {
      if (!value.toString.matches(Patterns.FLOAT_PATTERN)) {
        throw new RuntimeException("Invalid format for field: " + struct.name + " Type: " + struct.dataType + " Value: " + value)
      }
    }
    true
  }
}
