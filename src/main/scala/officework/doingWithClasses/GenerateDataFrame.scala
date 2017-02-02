package main.scala.officework.doingWithClasses

import main.scala.officework.doingWithObjects.Validations
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

/**
  * Created by ramaharjan on 2/1/17.
  */
class GenerateDataFrame extends scala.Serializable{

  val validators = new Validators

  //dataframe generation from input source using the schema generated
  def createDataFrame(sqlContext : SQLContext, inputLines : RDD[String], schema : StructType): DataFrame ={
    val rowFields = inputLines.map{line => line.split("\\^%~", -1)}.map{ array => Row.fromSeq(array.zip(schema.toSeq).map{ case (value, struct) => validators.convertTypes(value, struct) })}
    val df = sqlContext.createDataFrame(rowFields, schema)
    //    df.show()
    df
  }
}
