package officework.doingWithClasses.mara

import java.util

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.types.{DataType, DataTypes, StructField, StructType}
import org.apache.spark.sql.expressions.MutableAggregationBuffer

import scala.collection.immutable.HashMap
import scala.collection.mutable

/**
  * Created by ramaharjan on 3/9/17.
  */
class MaraUDAF(inputSourceSchema : StructType) extends UserDefinedAggregateFunction {
  var sourceSchema : StructType = _
  var bufferedSchema : StructType = _
  var returnDataType : DataType = DataTypes.createMapType(DataTypes.StringType, DataTypes.DoubleType)
  var mutableBuffer : MutableAggregationBuffer = _

  sourceSchema = inputSourceSchema

  //BufferSchema : This UDAF can hold calculated data in below mentioned buffers
  val bufferFields : util.ArrayList[StructField] = new util.ArrayList[StructField]
  val bufferStructField1 : StructField = DataTypes.createStructField("totalCount", DataTypes.DoubleType, true)
  bufferFields.add(bufferStructField1)
  val bufferStructField2 : StructField = DataTypes.createStructField("femaleCount",DataTypes.IntegerType, true)
  bufferFields.add(bufferStructField2)
  val bufferStructField3 :StructField  = DataTypes.createStructField("maleCount",DataTypes.IntegerType, true)
  bufferFields.add(bufferStructField3)
  val bufferStructField4 : StructField = DataTypes.createStructField("outputMap",DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType), true);
  bufferFields.add(bufferStructField4)
  bufferedSchema = DataTypes.createStructType(bufferFields)
  var testValue = 0.0

  /**
    * This method will determine the input schema of this UDAF
    */
  @Override
  def  inputSchema() : StructType =  sourceSchema

  /**
    * This method determines which bufferSchema will be used
    */
  @Override
  def bufferSchema : StructType = bufferedSchema

  /**
    * This method determines the return type of this UDAF
    */
  @Override
  def dataType : DataType = returnDataType

  /**
    * Returns true iff this function is deterministic, i.e. given the same input, always return the same output.
    */
  @Override
  def deterministic : Boolean = true

  /**
    * This method will re-initialize the variables and will be called only once for each group
    */
  @Override
  def initialize(buffer : MutableAggregationBuffer) : Unit = {
    testValue += 1.0
    buffer(0)=0.0
  }

  /**
    * This method is used to iterate between input rows
    */
  @Override
  def update(buffer : MutableAggregationBuffer, input : Row) : Unit = {
    buffer(0)= testValue
    println(testValue+" "+input.get(0) +" "+buffer.getDouble(0))
  }

  /**
    * This method will be used to merge data of two buffers
    */
  @Override
  def merge(buffer : MutableAggregationBuffer, input : Row) : Unit = {
    buffer.update(0, buffer.getDouble(0)+input.getDouble(0))
  }

  /**
    * This method calculates the final value by referring the aggregation buffer
    */
  @Override
  def evaluate(buffer : Row) : Any = {
    HashMap("prospectiveInpatient" -> buffer.getDouble(0), "prospectivePharmacy"->0.9)
  }
}