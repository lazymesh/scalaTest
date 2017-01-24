package src.Tests

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.types._

/**
  * Created by ramaharjan on 1/24/17.
  */
object DataFrames {

  //schema generating function reading from layout file
  def dynamicSchema(sc : SparkContext, file : String): StructType ={
    val readData = sc.textFile(file).filter(!_.startsWith("#"))
    val schema = readData.map(x=>x.split(";", -1)).map {value => StructField(value(1), dataType(value(4)))}
    val structType = StructType(schema.collect().toSeq)
    //    println(structType.prettyJson)
    structType
  }

  //dataframe generation from input source using the schema generated
  def eligDataFrame(sqlContext : SQLContext, inputLines : RDD[String], schema : StructType): DataFrame ={
    val rowFields = inputLines.map{line => line.split("\\^%~", -1)}.map{ array => Row.fromSeq(array.zip(schema.toSeq).map{ case (value, struct) => Validations.convertTypes(value, struct) })}
    val df = sqlContext.createDataFrame(rowFields, schema)
    //    df.show()
    df
  }

  //defining data type for schema according to layout
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
}
