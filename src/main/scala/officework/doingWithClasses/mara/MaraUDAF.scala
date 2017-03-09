package officework.doingWithClasses.mara

import java.util

import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.types.{DataType, DataTypes, StructField, StructType}
import org.apache.spark.sql.expressions.MutableAggregationBuffer

/**
  * Created by ramaharjan on 3/9/17.
  */
public class MaraUDAF extends UserDefinedAggregateFunction {
  var inputSchema : StructType;
  var bufferSchema : StructType;
  var bufferSchema : DataType;
  DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType);
  var mutableBuffer : MutableAggregationBuffer ;

  def MaraUDAF()
  {
    //inputSchema : This UDAF can accept 2 inputs which are of type Integer
    List<StructField> inputFields = new util.ArrayList<StructField>();
    StructField inputStructField1 = DataTypes.createStructField(“femaleCount”,DataTypes.IntegerType, true);
    inputFields.add(inputStructField1);
    StructField inputStructField2 = DataTypes.createStructField(“maleCount”,DataTypes.IntegerType, true);
    inputFields.add(inputStructField2);
    inputSchema = DataTypes.createStructType(inputFields);

    //BufferSchema : This UDAF can hold calculated data in below mentioned buffers
    List<StructField> bufferFields = new util.ArrayList<StructField>();
    StructField bufferStructField1 = DataTypes.createStructField(“totalCount”,DataTypes.IntegerType, true);
    bufferFields.add(bufferStructField1);
    StructField bufferStructField2 = DataTypes.createStructField(“femaleCount”,DataTypes.IntegerType, true);
    bufferFields.add(bufferStructField2);
    StructField bufferStructField3 = DataTypes.createStructField(“maleCount”,DataTypes.IntegerType, true);
    bufferFields.add(bufferStructField3);
    StructField bufferStructField4 = DataTypes.createStructField(“outputMap”,DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType), true);
    bufferFields.add(bufferStructField4);
    bufferSchema = DataTypes.createStructType(bufferFields);
  }

  /**
    * This method determines which bufferSchema will be used
    */
  @Override
  public StructType bufferSchema() {

    return bufferSchema;
  }

  /**
    * This method determines the return type of this UDAF
    */
  @Override
  public DataType dataType() {
    return returnDataType;
  }

  /**
    * Returns true iff this function is deterministic, i.e. given the same input, always return the same output.
    */
  @Override
  public boolean deterministic() {
    return true;
  }

  /**
    * This method will re-initialize the variables to 0 on change of city name
    */
  @Override
  public void initialize(MutableAggregationBuffer buffer) {
    buffer.update(0, 0);
    buffer.update(1, 0);
    buffer.update(2, 0);
    mutableBuffer = buffer;
  }

  /**
    * This method is used to increment the count for each city
    */
  @Override
  public void update(MutableAggregationBuffer buffer, Row input) {
    buffer.update(0, buffer.getInt(0) + input.getInt(0) + input.getInt(1));
    buffer.update(1, input.getInt(0));
    buffer.update(2, input.getInt(1));
  }

  /**
    * This method will be used to merge data of two buffers
    */
  @Override
  public void merge(MutableAggregationBuffer buffer, Row input) {

    buffer.update(0, buffer.getInt(0) + input.getInt(0));
    buffer.update(1, buffer.getInt(1) + input.getInt(1));
    buffer.update(2, buffer.getInt(2) + input.getInt(2));

  }

  /**
    * This method calculates the final value by referring the aggregation buffer
    */
  @Override
  public Object evaluate(Row buffer) {
    //In this method we are preparing a final map that will be returned as output
    Map<String,String> op = new HashMap<String,String>();
    op.put(“Total”, “” + mutableBuffer.getInt(0));
    op.put(“dominant”, “Male”);
    if(buffer.getInt(1) > mutableBuffer.getInt(2))
    {
      op.put(“dominant”, “Female”);
    }
    mutableBuffer.update(3,op);

    return buffer.getMap(3);
  }
  /**
    * This method will determine the input schema of this UDAF
    */
  @Override
  public StructType inputSchema() {

    return inputSchema;
  }

}