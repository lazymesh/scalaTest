import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructField, StructType}
import org.scalatest.{BeforeAndAfterEach, FunSuite}

/**
  * Created by ramaharjan on 3/10/17.
  */
class SchemaTests extends FunSuite with BeforeAndAfterEach {
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

  test("extracting partial schema"){
    val sparkContext = sparkSession.sparkContext
    val sqlContext = sparkSession.sqlContext
    import sqlContext.implicits._
    val df = Seq((1, 1, 2L), (1, 2, 3L), (1, 3, 4L), (2, 1, 5L)).toDF("one", "two", "three")
    val tempSchema = df.select("one", "three").schema
    println(tempSchema.prettyJson)
    sparkContext.stop
  }

  test("dynamically changing schema"){
    val sparkContext = sparkSession.sparkContext
    val sqlContext = sparkSession.sqlContext
    import sqlContext.implicits._
    val df = Seq((1, 1, 2L), (1, 2, 3L), (1, 3, 4L), (2, 1, 5L)).toDF("one", "two", "three")
    val newSchema = StructType(df.schema.map {
      case StructField( c, t, _, m) if c.equals("two") => StructField( c, t, nullable = true, m)
      case y: StructField => y
    })
    // apply new
    val newDf = df.sqlContext.createDataFrame( df.rdd, newSchema )
    println(newDf.schema.prettyJson)
    sparkContext.stop
  }
}
