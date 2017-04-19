import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.util.AccumulatorV2
import org.scalatest.{BeforeAndAfterEach, FunSuite}

/**
  * Created by ramaharjan on 4/18/17.
  */
class AccumulatorTest extends FunSuite with BeforeAndAfterEach {

  var sparkSession : SparkSession = _
  override def beforeEach() {
    sparkSession = SparkSession.builder().appName("udf testings")
      .master("local")
      .config("", "")
      .getOrCreate()
  }

  override def afterEach() {
    sparkSession.stop()
  }

  test("accumulator test"){
    val sparkContext = sparkSession.sparkContext
    val sqlContext = sparkSession.sqlContext
    import sqlContext.implicits._
    val diagdf = Seq(("239", "abrcd"), ("239.1", "abcd"), ("239.2", "abcd"), ("23.9", "abcd3"), ("239.5", "abcd")).toDF("diag1", "diagDesc")

    diagdf.show
    // Create an accumulator to store all questionable values.
    val heightValues = new StringAccumulator()
    sparkContext.register(heightValues)

    diagdf.foreach(row => validate(row))

    println(heightValues.value())

    // A function that checks for questionable values
    def validate(row: Row) = {
      val height = row.getString(row.fieldIndex("diagDesc"))
        heightValues.add(height.toString)
    }
  }


}

class StringAccumulator(private var _value: String) extends AccumulatorV2[String, String] {

  def this() {
    this("")
  }

  override def add(newValue: String): Unit = {
    _value = value + " " + newValue.trim
  }

  override def copy(): StringAccumulator = {
    new StringAccumulator(value)
  }

  override def isZero(): Boolean = {
    value.length() == 0
  }

  override def merge(other: AccumulatorV2[String, String]): Unit = {
    add(other.value)
  }

  override def reset(): Unit = {
    _value = ""
  }

  override def value(): String = {
    _value
  }
}
