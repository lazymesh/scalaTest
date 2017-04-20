package officework.doingWithClasses.framework.dataframeutils

import officework.doingWithClasses.framework.validation.Validators
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

import scala.io.Source

/**
  * Created by ramaharjan on 2/1/17.
  */
class GenerateDataFrame extends scala.Serializable{

  val validators = new Validators

  //dataframe generation from input source using the schema generated
  def createDataFrame(sqlContext : SQLContext, inputLines : RDD[String], schema : StructType, delimiter : String): DataFrame ={
    val rowFields = inputLines.map{line => line.split(delimiter, -1)}.map{ array => Row.fromSeq(array.zip(schema.toSeq).map{ case (value, struct) => validators.convertTypes(value.replace("\"", ""), struct) })}
    val df = sqlContext.createDataFrame(rowFields, schema)
    //    df.show()
    df
  }

  //dataframe generation from master tables source using the schema generated
  def createDataFrameFromResource(sparkContext : SparkContext, sqlContext : SQLContext, fileLocation : String, schema : StructType, delimiter : String): DataFrame ={
    val lines = Source.fromInputStream(getClass.getResourceAsStream(fileLocation)).getLines()
    val rowToRDD = sparkContext.parallelize(lines.toList)
    val rowFields = rowToRDD.map{line => line.split(delimiter, -1)}.map{ array => Row.fromSeq(array.zip(schema.toSeq).map{ case (value, struct) => validators.convertTypes(value.replace("\"", ""), struct) })}
    val df = sqlContext.createDataFrame(rowFields, schema)
    //    df.show()
    df
  }
}
