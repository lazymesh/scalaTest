package test

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.scalatest.{BeforeAndAfterEach, FunSuite}

/**
  * Created by ramaharjan on 3/3/17.
  */
class UnionTests extends FunSuite with BeforeAndAfterEach {

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

  test("merging/union testing of two dataframes of different size using sql query"){
    var firstTableColumns = Seq("diag1", "diagDesc")
    var firstTableNextColumns = Seq("diag2")
    var secondTableColumns = Seq("diagCode", "desc1")
    var finalSecond = secondTableColumns ++ firstTableNextColumns

    val sparkContext = sparkSession.sparkContext
    val sqlContext = sparkSession.sqlContext
    import sqlContext.implicits._

    var diagdf = Seq(("239", "", "1"),
      ("239.1", "", "2"),
      ("239.2", "", "3"),
      ("23.9", "", "4"),
      ("239.5", "", "4"))
      .toDF(firstTableColumns ++ firstTableNextColumns:_*)
    diagdf.createOrReplaceTempView("firstTable")

    val masterdf = Seq(("239", "dfsadf"),
      ("239.1", "dfsdf"),
      ("239.2", "sdfs"),
      ("23.9", "dfadf"),
      ("239.5", "dddd"))
      .toDF(secondTableColumns: _*)
    masterdf.createOrReplaceTempView("secondTable")

    diagdf = sqlContext.sql("select * from firstTable Union select *, null as diag2 from secondTable")

    diagdf.show

    sparkContext.stop
  }

  test("merging/union testing of two dataframes of different size using dataframe apis"){
    var firstTableColumns = Seq("diag1", "diagDesc")
    var firstTableNextColumns = Seq("diag2")
    var secondTableColumns = Seq("diagCode", "desc1")
    var finalSecond = secondTableColumns ++ firstTableNextColumns

    val sparkContext = sparkSession.sparkContext
    val sqlContext = sparkSession.sqlContext
    import sqlContext.implicits._

    var diagdf = Seq(("239", "", "1"),
      ("239.1", "", "2"),
      ("239.2", "", "3"),
      ("23.9", "", "4"),
      ("239.5", "", "4"))
      .toDF(firstTableColumns ++ firstTableNextColumns:_*)

    val masterdf = Seq(("239", "dfsadf"),
      ("239.1", "dfsdf"),
      ("239.2", "sdfs"),
      ("23.9", "dfadf"),
      ("239.5", "dddd"))
      .toDF(secondTableColumns: _*)

    diagdf = diagdf.select((firstTableColumns ++ firstTableNextColumns).map(col):_*)
    //    diagdf = diagdf.select(firstTableColumns.map(col):_*).union(masterdf.select(secondTableColumns.map(col):_*).("null as diag2"))

    diagdf.show

    sparkContext.stop
  }
}
