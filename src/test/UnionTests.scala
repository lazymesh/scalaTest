package test

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, lit}
import org.scalatest.{BeforeAndAfterEach, FunSuite}

/**
  * Created by ramaharjan on 3/3/17.
  */
class UnionTests extends FunSuite with BeforeAndAfterEach {

  def addColumns(dataFrame : DataFrame): DataFrame ={
    val columnsToAdd = Vector("diag2")

    var newDataFrame = dataFrame
    for(column <- 0 until columnsToAdd.size){
      newDataFrame = newDataFrame.withColumn(columnsToAdd(column), lit(""))
    }
    newDataFrame
  }

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
    var firstTableColumns = Vector("diag1", "diagDesc", "diag2")
    var secondTableColumns = Seq("diagCode", "desc1")
    var finalSecond = secondTableColumns

    val sparkContext = sparkSession.sparkContext
    val sqlContext = sparkSession.sqlContext
    import sqlContext.implicits._

    var diagdf = Seq(("239", "", "1"),
      ("239.1", "", "2"),
      ("239.2", "", "3"),
      ("23.9", "", "4"),
      ("239.5", "", "4"))
      .toDF(firstTableColumns:_*)

    var masterdf = Seq(("239", "dfsadf"),
      ("239.1", "dfsdf"),
      ("239.2", "sdfs"),
      ("23.9", "dfadf"),
      ("239.5", "dddd"))
      .toDF(secondTableColumns: _*)

    diagdf = diagdf.select((firstTableColumns).map(col):_*)
    masterdf = addColumns(masterdf)
    diagdf = diagdf.union(masterdf)

    diagdf.show

    sparkContext.stop
  }


}
