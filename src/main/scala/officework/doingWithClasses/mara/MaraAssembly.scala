package main.scala.officework.doingWithClasses.mara


import java.util

import officework.doingWithClasses.mara.{MaraUDAF, MaraUtils}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lit, row_number, udf}

import scala.collection.immutable.HashMap.HashTrieMap


/**
  * Created by ramaharjan on 3/2/17.
  */
class MaraAssembly(eligDataFrame : DataFrame, medDataFrame : DataFrame, rxDataFrame : DataFrame, endCycleDate : String, sc : SparkContext, sQLContext: SQLContext) {

  var eligDF = eligDataFrame
  var medDF = medDataFrame
  var rxDF = rxDataFrame
  MaraUtils.endOfCycleDate = endCycleDate

  val dfsWorkingDir = FileSystem.get(new Configuration()).getWorkingDirectory
  val mara1DatFile = "/mara3_9_0/MARA1.dat"
  val mara2DatFile = "/mara3_9_0/MARA2.dat"
  val maraLicenseFile = "/mara3_9_0/mara.lic"
  sc.addFile(dfsWorkingDir+mara1DatFile)
  sc.addFile(dfsWorkingDir+mara2DatFile)
  sc.addFile(dfsWorkingDir+maraLicenseFile)


  def setSortDate = udf((date: String) => date )

  def maraCalculator() : DataFrame = {

    //todo groupfilter of elig
    eligDF = eligDF.select(MaraUtils.eligRetain.map(col): _*)
    for (column <- 0 until MaraUtils.insertToEligColumns.size) {
      eligDF = eligDF.withColumn(MaraUtils.insertToEligColumns(column), lit(MaraUtils.insertToEligValues(column)))
    }
    eligDF = eligDF.withColumn(MaraUtils.sortDateColumn, setSortDate(eligDF("ins_med_term_date")))
    eligDF = eligDF.select(MaraUtils.finalOrderingColumns.map(col):_*)

    val window = Window.partitionBy(col("dw_member_id")).orderBy(col("ins_med_term_date").desc, col("ins_med_eff_date").desc, col("mbr_relationship_code").desc, col("ins_emp_group_id").desc)
    val latestEligDF = eligDF.withColumn("rn", row_number.over(window)).where(col("rn") === 1).drop(col("rn")).withColumn(MaraUtils.inputTypeFlagColumn(0), lit(MaraUtils.inputTypeFlagEligLatest(0)))

    //todo groupfilter of medical
    medDF = medDF.select(MaraUtils.medRetain.map(col): _*)
    for (column <- 0 until MaraUtils.insertToMedColumns.size) {
      medDF = medDF.withColumn(MaraUtils.insertToMedColumns(column), lit(MaraUtils.insertToMedValues(column)))
    }
    medDF = medDF.withColumn(MaraUtils.sortDateColumn, setSortDate(medDF("svc_service_frm_date")))
    //todo convert "svc_service_frm_date", "svc_service_to_date", "rev_paid_date", "ins_med_eff_date", "ins_med_term_date" to date
    medDF = medDF.select(MaraUtils.finalOrderingColumns.map(col):_*)

    //todo groupfilter of pharmacy
    rxDF = rxDF.withColumnRenamed("svc_service_frm_date", "rx_svc_filled_date")
    rxDF = rxDF.select(MaraUtils.rxRetain.map(col): _*)
    for (column <- 0 until MaraUtils.insertToRxColumns.size) {
      rxDF = rxDF.withColumn(MaraUtils.insertToRxColumns(column), lit(MaraUtils.insertToRxValues(column)))
    }
    rxDF = rxDF.withColumn(MaraUtils.sortDateColumn, setSortDate(rxDF("rx_svc_filled_date")))
    //todo convert "rx_svc_filled_date","ins_med_eff_date", "ins_med_term_date" to date
    rxDF = rxDF.select(MaraUtils.finalOrderingColumns.map(col):_*)

    var combined = latestEligDF.union(eligDF).union(medDF).union(rxDF)
    val maraUdaf = new MaraUDAF(combined.schema)
    combined = combined.orderBy("inputTypeFlag").groupBy("dw_member_id").agg(
      maraUdaf(MaraUtils.finalOrderingColumns.map(col):_*).as("maraOutput"))

    val maraSchema = combined.schema

    val temp = combined
    var temp2 = temp.rdd.map(row => {
      val list = Array.empty[String]
      list ++ row(0).toString
      val maraOutMap = row(1).asInstanceOf[Map[String, String]]
//      if(!maraOutMap.keySet.contains("emptyscore")) {
        list ++ maraOutMap.getOrElse("mbr_dob", "null")
        list ++ maraOutMap.getOrElse("mbr_relationship_code", "null")
        list ++ maraOutMap.getOrElse("mbr_relationship_desc", "null")
        list ++ maraOutMap.getOrElse("mbr_gender", "null")
        list ++ maraOutMap.getOrElse("unblindMemberId", "null")
        list ++ maraOutMap.getOrElse("mbr_current_status", "null")
        list ++ maraOutMap.getOrElse("memberFullName", "null")
        list ++ maraOutMap.getOrElse("ins_emp_group_id", "null")
        list ++ maraOutMap.getOrElse("ins_emp_group_name", "null")
        list ++ maraOutMap.getOrElse("ins_division_id", "null")
        list ++ maraOutMap.getOrElse("ins_carrier_id", "null")
        list ++ maraOutMap.getOrElse("ins_plan_id", "null")
        list ++ maraOutMap.getOrElse("udf16", "null")
        list ++ maraOutMap.getOrElse("udf17", "null")
        list ++ maraOutMap.getOrElse("udf18", "null")
        list ++ maraOutMap.getOrElse("udf19", "null")
        list ++ maraOutMap.getOrElse("udf20", "null")
        list ++ maraOutMap.getOrElse("udf21", "null")
        list ++ maraOutMap.getOrElse("udf22", "null")
        list ++ maraOutMap.getOrElse("udf23", "null")
        list ++ maraOutMap.getOrElse("udf24", "null")
        list ++ maraOutMap.getOrElse("udf25", "null")
        list ++ maraOutMap.getOrElse("ins_plan_type_code", "null")
        list ++ maraOutMap.getOrElse("integer_member_id", "null")
        list ++ maraOutMap.getOrElse("exposureMonths", "null")
        list ++ maraOutMap.getOrElse("prospectiveInpatientRaw", "null")
        list ++ maraOutMap.getOrElse("prospectiveOutpatientRaw", "null")
        list ++ maraOutMap.getOrElse("prospectiveMedicalRaw", "null")
        list ++ maraOutMap.getOrElse("prospectivePharmacyRaw", "null")
        list ++ maraOutMap.getOrElse("prospectivePhysicianRaw", "null")
        list ++ maraOutMap.getOrElse("prospectiveTotalScoreRaw", "null")
        list ++ maraOutMap.getOrElse("prospectiveERScoreRaw", "null")
        list ++ maraOutMap.getOrElse("prospectiveOtherScoreRaw", "null")
        list ++ maraOutMap.getOrElse("concurrentInpatientRaw", "null")
        list ++ maraOutMap.getOrElse("concurrentOutpatientRaw", "null")
        list ++ maraOutMap.getOrElse("concurrentMedicalRaw", "null")
        list ++ maraOutMap.getOrElse("concurrentPharmacyRaw", "null")
        list ++ maraOutMap.getOrElse("concurrentPhysicianRaw", "null")
        list ++ maraOutMap.getOrElse("concurrentTotalScoreRaw", "null")
        list ++ maraOutMap.getOrElse("concurrentERScoreRaw", "null")
        list ++ maraOutMap.getOrElse("concurrentOtherScoreRaw", "null")
        list ++ maraOutMap.getOrElse("conditionList", "null")
        list ++ maraOutMap.getOrElse("groupWiseAmounts", "null")
        list ++ maraOutMap.getOrElse("totalPaidAmount", "null")
        list ++ maraOutMap.getOrElse("totalAllowedAmount", "null")
//      }
    list
    })
    Seq(temp2).foreach(println)
    import sQLContext.implicits._
    Seq((temp2.toLocalIterator).toList).toDF(MaraUtils.maraRawOutput:_*).show(50)

    combined
  }
}