import officework.doingWithClasses.mara.MaraUtils
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterEach, FunSuite}

/**
  * Created by ramaharjan on 4/20/17.
  */
class OptionTest extends FunSuite with BeforeAndAfterEach{
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

  test("simple double test"){
    val data = "1"
    println("LLLLLLLLLLLLLLLLL "+data.toDouble)
  }

  test("option test"){
    val value = null
    val print = Option(value) getOrElse("nulddddl value")
    println(print)

  }

}
