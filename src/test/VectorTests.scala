import officework.doingWithClasses.mara.MaraUtils
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterEach, FunSuite}

/**
  * Created by ramaharjan on 3/13/17.
  */
class VectorTests extends FunSuite with BeforeAndAfterEach{
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

  test("get index of vector by calling name"){
    val maraFinalFields = MaraUtils.finalOrderingColumns
    val billedAmtIndex = maraFinalFields.indexOf("rev_billed_amt")
    val paidAmtIndex = maraFinalFields.indexOf("rev_paid_amt")
    val allowedAmtIndex = maraFinalFields.indexOf("rev_allowed_amt")
    println(billedAmtIndex)
    println(paidAmtIndex)
    println(allowedAmtIndex)

  }

}
