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
  var returnDataType : DataType = DataTypes.createMapType(DataTypes.StringType, DataTypes.createArrayType(DataTypes.StringType))
  var mutableBuffer : MutableAggregationBuffer = _

  sourceSchema = inputSourceSchema

  //BufferSchema : This UDAF can hold calculated data in below mentioned buffers
  val bufferFields : util.ArrayList[StructField] = new util.ArrayList[StructField]
  val bufferStructField0 : StructField = DataTypes.createStructField("memberInfo", DataTypes.createArrayType(DataTypes.StringType, true), true)
  bufferFields.add(bufferStructField0)
  val bufferStructField1 : StructField = DataTypes.createStructField("isMemberActive", DataTypes.BooleanType, true)
  bufferFields.add(bufferStructField1)
  val bufferStructField2 : StructField = DataTypes.createStructField("eligibleDates", DataTypes.createMapType(DataTypes.LongType, DataTypes.LongType), true)
  bufferFields.add(bufferStructField2)
  val bufferStructField3 : StructField = DataTypes.createStructField("rxclaims", DataTypes.StringType, true)
  bufferFields.add(bufferStructField3)
  val bufferStructField4 : StructField = DataTypes.createStructField("rxclaimsArrayList", DataTypes.createArrayType(DataTypes.StringType, true), true)
  bufferFields.add(bufferStructField4)
  val bufferStructField7 : StructField = DataTypes.createStructField("outputMap",DataTypes.createMapType(DataTypes.StringType,DataTypes.createArrayType(DataTypes.StringType)), true)
  bufferFields.add(bufferStructField7)
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
    println(":::::::::::::::::: ")
    maraBuffer = new MaraBuffer
    buffer(0) = new util.ArrayList[String]
    buffer(1) = false
    buffer(2) = new mutable.HashMap[Long, Long]
    buffer(3) = ""
    buffer(4) = new util.ArrayList[String]
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
    if(buffer.getBoolean(1) || input.getBoolean(1)){
      buffer.update(1, true)
    }
    buffer.update(2, buffer.getMap(2) ++ input.getMap(2))
    val memberId = if(!buffer.getString(3).isEmpty) buffer.getString(3) else input.getString(3)

    buffer.update(4, (buffer.getList(4).toArray ++ (input.getList(4).toArray())))
//    println(buffer.getString(3)+" "+input.getString(3)+" "+buffer.getMap(2)+"LLLLLLLLLLLLLLLLLLLLLLLLLLL"+input.getMap(2))


  }

  /**
    * This method calculates the final value by referring the aggregation buffer
    */
  @Override
  def evaluate(buffer : Row) : Any = {
    println(buffer.getString(3)+" eeeeeeeee "+buffer.getMap(2)+" "+buffer.getBoolean(1)+" "+buffer.getList(4))
    HashMap("prospectiveInpatient" -> buffer.getList(0))
  }
}