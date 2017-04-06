package officework.doingWithClasses.mara

import java.util

import main.scala.officework.doingWithObjects.DateUtils
import officework.doingWithClasses.SummableMap
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DataTypes, StructField, StructType}

import scala.collection.mutable

/**
  * Created by ramaharjan on 3/9/17.
  */
class MaraUDAF(inputSourceSchema : StructType) extends UserDefinedAggregateFunction {
  var sourceSchema : StructType = _
  var bufferedSchema : StructType = _
  var returnDataType : DataType = DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType)
  var mutableBuffer : MutableAggregationBuffer = _

  sourceSchema = inputSourceSchema

  //BufferSchema : This UDAF can hold calculated data in below mentioned buffers
  val bufferFields : util.ArrayList[StructField] = new util.ArrayList[StructField]
  val bufferStructField0 : StructField = DataTypes.createStructField("memberInfo", DataTypes.createArrayType(DataTypes.StringType, true), true)
  bufferFields.add(bufferStructField0)
  val bufferStructField1 : StructField = DataTypes.createStructField("isMemberActive", DataTypes.BooleanType, true)
  bufferFields.add(bufferStructField1)
  val bufferStructField2 : StructField = DataTypes.createStructField("eligibleDates", DataTypes.createMapType(DataTypes.LongType, DataTypes.LongType, true), true)
  bufferFields.add(bufferStructField2)
  val bufferStructField3 : StructField = DataTypes.createStructField("rxClaimsArrayList", DataTypes.createArrayType(DataTypes.StringType, true), true)
  bufferFields.add(bufferStructField3)
  val bufferStructField4 : StructField = DataTypes.createStructField("medClaimsArrayList", DataTypes.createArrayType(DataTypes.StringType, true), true)
  bufferFields.add(bufferStructField4)
  val bufferStructField5 : StructField = DataTypes.createStructField("groupWisePaidAmount", DataTypes.createMapType(DataTypes.StringType, DataTypes.DoubleType, true), true)
  bufferFields.add(bufferStructField5)
  val bufferStructField6 : StructField = DataTypes.createStructField("groupWisePaidAllowedAmount", DataTypes.createMapType(DataTypes.StringType, DataTypes.DoubleType, true), true)
  bufferFields.add(bufferStructField6)
  bufferedSchema = DataTypes.createStructType(bufferFields)
  var maraBuffer : MaraBuffer = new MaraBuffer()

  // This is the input fields for your aggregate function.
  override def  inputSchema() : StructType =  sourceSchema

  /**
    * This method determines which bufferSchema will be used
    */
  override def bufferSchema : StructType = bufferedSchema

  /**
    * This method determines the return type of this UDAF
    */
  override def dataType : DataType = returnDataType

  /**
    * Returns true iff this function is deterministic, i.e. given the same input, always return the same output.
    */
  override def deterministic : Boolean = true

  /**
    * This method will re-initialize the variables and will be called only once for each group
    */
  override def initialize(buffer : MutableAggregationBuffer) : Unit = {
    maraBuffer.initializeOrClear
    buffer(0) = new util.ArrayList[String]
    buffer(1) = false
    buffer(2) = new mutable.HashMap[Long, Long]
    buffer(3) = new util.ArrayList[String]
    buffer(4) = new util.ArrayList[String]
    buffer(5) = new mutable.HashMap[String, Double]
    buffer(6) = new mutable.HashMap[String, Double]
  }

  /**
    * This method is used to iterate between input rows
    */
  override def update(buffer : MutableAggregationBuffer, input : Row) : Unit = {
    maraBuffer.populate(buffer, input)
    println("**********8888 "+buffer(5).asInstanceOf[Map[String, Double]].getOrElse("02", "empty"))
  }

  /**
    * This method will be used to merge data of two buffers
    */
  override def merge(buffer : MutableAggregationBuffer, input : Row) : Unit = {
    buffer.update(0, buffer.getList(0).toArray() ++ input.getList(0).toArray()) //eligibility info
    if(buffer.getBoolean(1) || input.getBoolean(1)){
      buffer.update(1, true) // activeness of a member within one years time
    }
    buffer.update(2, buffer.getMap(2) ++ input.getMap(2)) //map for exposure months
    buffer.update(3, buffer.getList(3).toArray() ++ input.getList(3).toArray()) //pharmacy claim list to be passed to mara
    buffer.update(4, buffer.getList(4).toArray() ++ input.getList(4).toArray()) //medical claim list to be passed to mara

    //    buffer.update(5, buffer.getMap(5) ++ input.getMap(5)) //map for groupwise paid amount
    if(!buffer.getList(0).isEmpty) {
      println("RRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRR " + buffer.getList(0).get(0))
    }
    buffer.update(5, mergeAmounts(buffer.getMap(5).asInstanceOf[Map[String, Double]], input.getMap(5).asInstanceOf[Map[String, Double]])) //map for groupwise paid amount
    buffer.update(6, mergeAmounts(buffer.getMap(6).asInstanceOf[Map[String, Double]], input.getMap(6).asInstanceOf[Map[String, Double]])) //map for groupwise allowed amount
  }

  /**
    * This method calculates the final value by referring the aggregation buffer
    */
  override def evaluate(buffer : Row) : Any = {
    if(buffer.getBoolean(1)) {
      maraBuffer.calculateMaraScores(buffer)
    }
    else
      mutable.HashMap("emptyscore" -> "")
  }

  def mergeAmounts(buffer : Map[String, Double], input : Map[String, Double]) : SummableMap[String, Double] ={

    //    println(buffer(5).asInstanceOf[Map[String, Double]].getOrElse("02", "empty")+" eeeeeeeeeeeee "+input(5).asInstanceOf[Map[String, Double]].getOrElse("02", "empty"))
    val amount : SummableMap[String, Double] = new SummableMap[String, Double]
    var prevPaidAmountMap = buffer
    for(map <- prevPaidAmountMap) {
      amount.put(map._1, map._2)
      //      println(map._1+" &&&&&&&&&&&&&&& "+amount.getOrElse(map._1, "empty"))
    }
    val newPaidAmountMap = input
    for(map <- newPaidAmountMap){
      amount.put(map._1, map._2)
      //      println(map._1+" ^^^^^^^^^^^^^^^^^^ "+amount.getOrElse(map._1, "empty"))
    }
    amount
  }
}