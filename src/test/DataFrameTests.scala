package test

import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.{BeforeAndAfterEach, FunSuite}
import org.apache.spark.sql.functions._

/**
  * Created by ramaharjan on 2/2/17.
  */
class DataFrameTests extends FunSuite with BeforeAndAfterEach {

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

  test("updating a table using another tables value "){
    val sparkContext = sparkSession.sparkContext
    val sqlContext = sparkSession.sqlContext
    import sqlContext.implicits._
    var diagdf = Seq(("239", "", "1"), ("239.1", "", "2"), ("239.2", "", "3"), ("23.9", "", "4"), ("239.5", "", "4")).toDF("diag1", "diagDesc", "diag2")
    val masterdf = Seq(("239", "dfsadf"), ("239.1", "dfsdf"), ("239.2", "sdfs"), ("23.9", "dfadf"), ("239.5", "dddd")).toDF("diagCode", "desc1")

    diagdf = diagdf.join(masterdf, diagdf("diag1") === masterdf("diagCode"), "left")
      .withColumn("diagDesc", masterdf("desc1"))
      .drop("diagCode", "desc1")
    diagdf.show

    sparkContext.stop
  }

  test("update a dataframe using another dataframe"){
    val sparkContext = sparkSession.sparkContext
    val sqlContext = sparkSession.sqlContext
    import sqlContext.implicits._
    // select only diag codes from medical table
    var diagCodes = Seq(
      ("0", "1", "2", "3", "4", "0", "6", "3", "9"),
      ("1", "3", "4", "5", "1", "3", "4", "5", "1"),
      ("2", "6", "6", "8", "2", "6", "6", "8", "2"),
      ("3", "9", "9", "1", "3", "9", "9", "1", "3"))
      .toDF("svc_diag_1_code", "svc_diag_2_code", "svc_diag_3_code", "svc_diag_4_code", "svc_diag_5_code",
        "svc_diag_6_code", "svc_diag_7_code", "svc_diag_8_code", "svc_diag_9_code")

    //select only groupers and supergroupers with diagcode from master table
    val masterdf = Seq(
      ("0", "", "grouperDescription0", "superGrouperID0", "superGrouperDescription0"),
      ("1", "", "grouperDescription1", "superGrouperID1", "superGrouperDescription1"),
      ("2", "grouperID2", "grouperDescription2", "superGrouperID2", "superGrouperDescription2"),
      ("3", "grouperID3", "grouperDescription3", "superGrouperID3", "superGrouperDescription3"),
      ("4", "grouperID4", "grouperDescription4", "superGrouperID4", "superGrouperDescription4"),
      ("5", "grouperID5", "grouperDescription5", "superGrouperID5", "superGrouperDescription5"),
      ("6", "grouperID6", "grouperDescription6", "superGrouperID6", "superGrouperDescription6"),
      ("7", "grouperID7", "grouperDescription7", "superGrouperID7", "superGrouperDescription7"),
      ("8", "grouperID8", "grouperDescription8", "superGrouperID8", "superGrouperDescription8"),
      ("9", "grouperID9", "grouperDescription9", "superGrouperID9", "superGrouperDescription9"))
      .toDF("diagnosisCode", "grouperID", "grouperDescription", "superGrouperID", "superGrouperDescription")

    def diagnosisMasterTableUdfs = udf((value : String) =>{
      if(!value.isEmpty) value else "Ungroupable"
    })
     for(i <- 1 to 9){
       diagCodes = diagCodes.join(masterdf, diagCodes("svc_diag_"+i+"_code") === masterdf("diagnosisCode"), "left")
         .withColumn("diag"+i+"_grouper_id", diagnosisMasterTableUdfs(masterdf("grouperID")))
         .withColumn("diag"+i+"_grouper_desc", diagnosisMasterTableUdfs(masterdf("grouperDescription")))
         .withColumn("diag"+i+"_supergrouper_id", diagnosisMasterTableUdfs(masterdf("superGrouperID")))
         .withColumn("diag"+i+"_supergrouper_desc", diagnosisMasterTableUdfs(masterdf("superGrouperDescription")))
      .drop("diagnosisCode", "grouperID", "grouperDescription", "superGrouperID", "superGrouperDescription")
     }


    diagCodes.show
/*

    // I think `t2` is an alias for `time` and you want to update `t2`
    val time = Seq(
      (1, 10),
      (4, 40),
      (9, 90)).toDF("diagnosisCode", "grouperID", "grouperDescription", "superGrouperID", "superGrouperDescription")

    // this is the calculation of the new records
    val new_t2 = u.join(time)
      .where('time >= 'start)
      .where('time < 'end)
      .withColumn("recordings + c", 'recordings + 'c)
//      .select('time, $"recordings + c" as 'recordings)

    // the following is an equivalent of INSERT INTO using Dataset API
//    val solution = time.union(new_t2)
    new_t2.show
    //    time.show
//    solution.show
*/

    sparkContext.stop()
  }
}
