package main.scala.officework.doingWithClasses

import org.apache.spark.sql.types._

import scala.io.Source

/**
  * Created by ramaharjan on 2/1/17.
  */
class GenerateSchemas {

  //schema generating function reading from layout file
  def dynamicSchema(file : String): StructType ={
    val readData = Source.fromFile(file).getLines().filter(!_.startsWith("#"))
    val schema = readData.map(x=>x.split(";", -1)).map {value => StructField(value(1), dataType(value(4)))}
    val structType = StructType(schema.toSeq)
    //    println(structType.prettyJson)
    structType
  }

  //defining data type for schema according to layout
  def dataType(dataType : String) : DataType ={
    if(dataType.equalsIgnoreCase("int")){
      IntegerType
    }
    else if(dataType.equalsIgnoreCase("date")){
      DateType
    }
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
