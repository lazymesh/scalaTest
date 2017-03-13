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
  val bufferStructField1 : StructField = DataTypes.createStructField("memberInfo", DataTypes.createArrayType(DataTypes.StringType), true)
  bufferFields.add(bufferStructField1)
  val bufferStructField4 : StructField = DataTypes.createStructField("outputMap",DataTypes.createMapType(DataTypes.StringType, DataTypes.DoubleType), true);
  bufferFields.add(bufferStructField4)
  bufferedSchema = DataTypes.createStructType(bufferFields)
  var maraBuffer : MaraBuffer = _

  // This is the input fields for your aggregate function.
  override def  inputSchema() : StructType =  sourceSchema

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
    maraBuffer = new MaraBuffer
    buffer(0)= new util.ArrayList[String]
  }

  /**
    * This method is used to iterate between input rows
    */
  @Override
  def update(buffer : MutableAggregationBuffer, input : Row) : Unit = {
    maraBuffer.populate(buffer, input)
  }

  /**
    * This method will be used to merge data of two buffers
    */
  @Override
  def merge(buffer : MutableAggregationBuffer, input : Row) : Unit = {
    val mergedList = (buffer.getList(0).toArray()) ++ (input.getList(0)).toArray()
    buffer.update(0, mergedList)
    println(buffer.getList(0)+"(((((((((((((((((((((((((((((((((((((( "+input.getList(0))
  }

  /**
    * This method calculates the final value by referring the aggregation buffer
    */
  @Override
  def evaluate(buffer : Row) : Any = {
    println("LLLLLLLLLL "+buffer.getList(0))
    HashMap("prospectiveInpatient" -> 2.0, "prospectivePharmacy"->0.9)
  }
}