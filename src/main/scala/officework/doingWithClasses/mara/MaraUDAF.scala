package officework.doingWithClasses.mara

import java.util

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.types.{DataType, DataTypes, StructField, StructType}
import org.apache.spark.sql.expressions.MutableAggregationBuffer

import scala.collection.mutable

/**
  * Created by ramaharjan on 3/9/17.
  */
class MaraUDAF extends UserDefinedAggregateFunction {
  var sourceSchema : StructType = _
  var bufferedSchema : StructType = _
  var returnDataType : DataType = DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType)
  var mutableBuffer : MutableAggregationBuffer = _

  def MaraUDAF(inputSchema : StructType)
  {
    //inputSchema : This UDAF can accept 2 inputs which are of type Integer
    val inputFields : util.ArrayList[StructField] = new util.ArrayList[StructField]
    val inputStructField1 : StructField = DataTypes.createStructField("femaleCount",DataTypes.IntegerType, true)
    inputFields.add(inputStructField1)
    val inputStructField2 : StructField = DataTypes.createStructField("maleCount", DataTypes.IntegerType, true)
    inputFields.add(inputStructField2)
    sourceSchema = DataTypes.createStructType(inputFields)

    //BufferSchema : This UDAF can hold calculated data in below mentioned buffers
    val bufferFields : util.ArrayList[StructField] = new util.ArrayList[StructField]
    val bufferStructField1 : StructField = DataTypes.createStructField("totalCount", DataTypes.IntegerType, true)
    bufferFields.add(bufferStructField1)
    val bufferStructField2 : StructField = DataTypes.createStructField("femaleCount",DataTypes.IntegerType, true)
    bufferFields.add(bufferStructField2)
    val bufferStructField3 :StructField  = DataTypes.createStructField("maleCount",DataTypes.IntegerType, true)
    bufferFields.add(bufferStructField3)
    val bufferStructField4 : StructField = DataTypes.createStructField("outputMap",DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType), true);
    bufferFields.add(bufferStructField4)
    bufferedSchema = DataTypes.createStructType(bufferFields)
  }

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
    * This method will re-initialize the variables to 0 on change of city name
    */
  @Override
  def initialize(buffer : MutableAggregationBuffer) : Unit = {
    buffer.update(0, 0);
    buffer.update(1, 0);
    buffer.update(2, 0);
    mutableBuffer = buffer;
  }

  /**
    * This method is used to increment the count for each city
    */
  @Override
  def update(buffer : MutableAggregationBuffer, input : Row) : Unit = {
    buffer.update(0, buffer.getInt(0) + input.getInt(0) + input.getInt(1));
    buffer.update(1, input.getInt(0));
    buffer.update(2, input.getInt(1));
  }

  /**
    * This method will be used to merge data of two buffers
    */
  @Override
  def merge(buffer : MutableAggregationBuffer, input : Row) : Unit = {
    buffer.update(0, buffer.getInt(0) + input.getInt(0));
    buffer.update(1, buffer.getInt(1) + input.getInt(1));
    buffer.update(2, buffer.getInt(2) + input.getInt(2));

  }

  /**
    * This method calculates the final value by referring the aggregation buffer
    */
  @Override
  def evaluate(buffer : Row) : Any = {
    //In this method we are preparing a final map that will be returned as output
    var op : mutable.HashMap[String, Any]  = mutable.HashMap.empty[String, Any]
    op += ("Total" -> mutableBuffer.getInt(0))
    op += ("dominant" -> "Male")
    if(buffer.getInt(1) > mutableBuffer.getInt(2))
    {
      op += ("dominant" -> "Female")
    }
    mutableBuffer.update(3,op)

    buffer.getMap(3)
  }
  /**
    * This method will determine the input schema of this UDAF
    */
  @Override
  def  inputSchema() : StructType =  sourceSchema

}