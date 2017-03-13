import java.util

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{StructType, _}
import org.scalatest.{BeforeAndAfterEach, FunSuite}
import org.apache.spark.sql.functions._

import scala.collection.mutable

/**
  * Created by ramaharjan on 3/9/17.
  */
class UDAFTests extends FunSuite with BeforeAndAfterEach {
  var sparkSession : SparkSession = _
  override def beforeEach() {
    sparkSession = SparkSession.builder().appName("udaf testings")
      .master("local")
      .config("", "")
      .getOrCreate()
  }

  override def afterEach() {
    sparkSession.stop()
  }

  test("testing udaf "){
    val sparkContext = sparkSession.sparkContext
    val sqlContext = sparkSession.sqlContext
    import sqlContext.implicits._

    // create an RDD of tuples with some data
    val custs = Seq(
      (1, "Widget Co", 120000.00, 0.00, "AZ"),
      (2, "Acme Widgets", 410500.00, 500.00, "CA"),
      (3, "Widgetry", 200.00, 200.00, "CA"),
      (4, "Widgets R Us", 410500.00, 0.0, "CA"),
      (5, "Ye Olde Widgete", 500.00, 0.0, "MA")
    )
    val customerRows = sparkContext.parallelize(custs, 4)
    val customerDF = customerRows.toDF("id", "name", "sales", "discount", "state")
    customerDF.show

    val mysum = new ScalaAggregateFunction()

    // register as a temporary table
    customerDF.createOrReplaceTempView("customers")
    sqlContext.udf.register("mysum", mysum)

    // now use it in a query
//    val sqlResult = sqlContext.sql(s"SELECT state, mysum(sales) AS bigsales FROM customers GROUP BY state").show()

    //this is the second way using dataframe
    val dfResult = customerDF.groupBy(col("state")).agg(mysum(col("sales")).as("bigsales")).show
  }

  test("complex udaf test"){

  }

  test("testing udaf for multiple output"){
    //Set up sparkContext and SQLContext
    val sparkContext = sparkSession.sparkContext
    val sqlContext = sparkSession.sqlContext
    import sqlContext.implicits._

    // create an RDD of tuples with some data
    var populationDF = Seq(
      ("Kathmandu", 1200, 2000),
      ("Bhaktapur", 410, 500),
      ("Patan", 200, 200),
      ("Dhulikhel", 410, 390),
      ("Pokhara", 500, 710)
    ).toDF("city", "Female", "Male")

/*    val newSchema = StructType(populationDF.schema.map {
      case StructField( c, t, _, m) if (c.equals("Female") || (c.equals("Male"))) => StructField( c, IntegerType, nullable = true, m)
      case y: StructField => y
    })

    populationDF = sqlContext.createDataFrame(populationDF.rdd, newSchema)*/
    //Register our Spark UDAF
    val sparkUDAF = new SparkUDAF()
    sqlContext.udf.register("uf",sparkUDAF)
    populationDF.show

    //Register dataframe as table
    populationDF.createOrReplaceTempView("cities")

    //Run query
//    sqlContext.sql("SELECT city , count['dominant'] as Dominant, count['Total'] as Total from(select city, uf(Female,Male) as count from cities group by (city)) temp").show(false)
    val udaf = sparkUDAF(col("Female"), col("Male"))
    val tempdf = populationDF.groupBy(col("city")).agg(udaf("dominant").as("Dominant"), udaf("Total").as("Total"))
    tempdf.show
  }
}

private class ScalaAggregateFunction extends UserDefinedAggregateFunction {

  // an aggregation function can take multiple arguments in general. but
  // this one just takes one
  def inputSchema: StructType = new StructType().add("sales", DoubleType)
  // the aggregation buffer can also have multiple values in general but
  // this one just has one: the partial sum
  def bufferSchema: StructType = new StructType().add("sumLargeSales", DoubleType)
  // returns just a double: the sum
  def dataType: DataType = DoubleType
  // always gets the same result
  def deterministic: Boolean = true

  // each partial sum is initialized to zero
  def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer.update(0, 0.0)
  }

  // an individual sales value is incorporated by adding it if it exceeds 500.0
  def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val sum = buffer.getDouble(0)
    if (!input.isNullAt(0)) {
      val sales = input.getDouble(0)
      if (sales > 500.0) {
        buffer.update(0, sum+sales)
      }
    }
  }

  // buffers are merged by adding the single values in them
  def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    println(":::::::::::::::::::::::::::::"+buffer1.getDouble(0)+" "+buffer2.getDouble(0))
    buffer1.update(0, buffer1.getDouble(0) + buffer2.getDouble(0))
  }

  // the aggregation buffer just has one value: so return it
  def evaluate(buffer: Row): Any = {
    buffer.getDouble(0)
  }
}

class SparkUDAF() extends UserDefinedAggregateFunction {
  var sourceSchema : StructType = _
  var bufferedSchema : StructType = _
  var returnDataType : DataType = DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType)
  var mutableBuffer : MutableAggregationBuffer = _

  //inputSchema : This UDAF can accept 2 inputs which are of type Integer
  val inputFields : util.ArrayList[StructField] = new util.ArrayList[StructField]
  val inputStructField1 : StructField = DataTypes.createStructField("femaleCount",DataTypes.IntegerType, true)
  inputFields.add(inputStructField1)
  val inputStructField2 : StructField = DataTypes.createStructField("maleCount", DataTypes.IntegerType, true)
  inputFields.add(inputStructField2)
  sourceSchema = DataTypes.createStructType(inputFields)

  //BufferSchema : This UDAF can hold calculated data in below mentioned buffers
  val bufferFields : util.ArrayList[StructField] = new util.ArrayList[StructField]
  val bufferStructField1 : StructField = DataTypes.createStructField("totalCount", DataTypes.IntegerType, false)
  bufferFields.add(bufferStructField1)
  val bufferStructField2 : StructField = DataTypes.createStructField("femaleCount",DataTypes.IntegerType, false)
  bufferFields.add(bufferStructField2)
  val bufferStructField3 :StructField  = DataTypes.createStructField("maleCount",DataTypes.IntegerType, false)
  bufferFields.add(bufferStructField3)
  val bufferStructField4 : StructField = DataTypes.createStructField("outputMap",DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType), true);
  bufferFields.add(bufferStructField4)
  bufferedSchema = DataTypes.createStructType(bufferFields)

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
    buffer.update(0, 0)
    buffer.update(1, 0)
    buffer.update(2, 0)
    mutableBuffer = buffer
  }

  /**
    * This method is used to increment the count for each city
    */
  @Override
  def update(buffer : MutableAggregationBuffer, input : Row) : Unit = {
    buffer.update(0, buffer.getInt(0) + input.getInt(0) + input.getInt(1))
    buffer.update(1, input.getInt(0))
    buffer.update(2, input.getInt(1))
  }

  /**
    * This method will be used to merge data of two buffers
    */
  @Override
  def merge(buffer : MutableAggregationBuffer, input : Row) : Unit = {
    buffer.update(0, buffer.getInt(0) + input.getInt(0))
    buffer.update(1, buffer.getInt(1) + input.getInt(1))
    buffer.update(2, buffer.getInt(2) + input.getInt(2))
  }

  /**
    * This method calculates the final value by referring the aggregation buffer
    */
  @Override
  def evaluate(buffer : Row) : Any = {
    //In this method we are preparing a final map that will be returned as output
    var op : mutable.HashMap[String, Any]  = mutable.HashMap.empty[String, Any]
    op += ("Total" -> mutableBuffer.getInt(0).toString)
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
