package main.scala.officework.doingWithClasses.mara


import officework.doingWithClasses.mara.{MaraUDAF, MaraUtils}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lit, row_number, udf}


/**
  * Created by ramaharjan on 3/2/17.
  */
class MaraAssembly(eligDataFrame : DataFrame, medDataFrame : DataFrame, rxDataFrame : DataFrame, endCycleDate : String, sc : SparkContext) {

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
      maraUdaf(MaraUtils.finalOrderingColumns.map(col):_*)("mbr_dob").as("mbr_dob"),
      maraUdaf(MaraUtils.finalOrderingColumns.map(col):_*)("mbr_relationship_code").as("mbr_relationship_code"),
      maraUdaf(MaraUtils.finalOrderingColumns.map(col):_*)("mbr_relationship_desc").as("mbr_relationship_desc"),
      maraUdaf(MaraUtils.finalOrderingColumns.map(col):_*)("mbr_gender").as("mbr_gender"),
      maraUdaf(MaraUtils.finalOrderingColumns.map(col):_*)("unblindMemberId").as("unblindMemberId"),
      maraUdaf(MaraUtils.finalOrderingColumns.map(col):_*)("mbr_current_status").as("mbr_current_status"),
      maraUdaf(MaraUtils.finalOrderingColumns.map(col):_*)("memberFullName").as("memberFullName"),
      maraUdaf(MaraUtils.finalOrderingColumns.map(col):_*)("ins_emp_group_id").as("ins_emp_group_id"),
      maraUdaf(MaraUtils.finalOrderingColumns.map(col):_*)("ins_emp_group_name").as("ins_emp_group_name"),
      maraUdaf(MaraUtils.finalOrderingColumns.map(col):_*)("ins_division_id").as("ins_division_id"),
      maraUdaf(MaraUtils.finalOrderingColumns.map(col):_*)("ins_carrier_id").as("ins_carrier_id"),
      maraUdaf(MaraUtils.finalOrderingColumns.map(col):_*)("ins_plan_id").as("ins_plan_id"),
      maraUdaf(MaraUtils.finalOrderingColumns.map(col):_*)("udf16").as("udf16"),
      maraUdaf(MaraUtils.finalOrderingColumns.map(col):_*)("udf17").as("udf17"),
      maraUdaf(MaraUtils.finalOrderingColumns.map(col):_*)("udf18").as("udf18"),
      maraUdaf(MaraUtils.finalOrderingColumns.map(col):_*)("udf19").as("udf19"),
      maraUdaf(MaraUtils.finalOrderingColumns.map(col):_*)("udf20").as("udf20"),
      maraUdaf(MaraUtils.finalOrderingColumns.map(col):_*)("udf21").as("udf21"),
      maraUdaf(MaraUtils.finalOrderingColumns.map(col):_*)("udf22").as("udf22"),
      maraUdaf(MaraUtils.finalOrderingColumns.map(col):_*)("udf23").as("udf23"),
      maraUdaf(MaraUtils.finalOrderingColumns.map(col):_*)("udf24").as("udf24"),
      maraUdaf(MaraUtils.finalOrderingColumns.map(col):_*)("udf25").as("udf25"),
      maraUdaf(MaraUtils.finalOrderingColumns.map(col):_*)("ins_plan_type_code").as("ins_plan_type_code"),
      maraUdaf(MaraUtils.finalOrderingColumns.map(col):_*)("integer_member_id").as("integer_member_id"),
      maraUdaf(MaraUtils.finalOrderingColumns.map(col):_*)("exposureMonths").as("exposureMonths"),
      maraUdaf(MaraUtils.finalOrderingColumns.map(col):_*)("prospectiveInpatientRaw").as("prospectiveInpatientRaw"),
      maraUdaf(MaraUtils.finalOrderingColumns.map(col):_*)("prospectiveOutpatientRaw").as("prospectiveOutpatientRaw"),
      maraUdaf(MaraUtils.finalOrderingColumns.map(col):_*)("prospectiveMedicalRaw").as("prospectiveMedicalRaw"),
      maraUdaf(MaraUtils.finalOrderingColumns.map(col):_*)("prospectivePharmacyRaw").as("prospectivePharmacyRaw"),
      maraUdaf(MaraUtils.finalOrderingColumns.map(col):_*)("prospectivePhysicianRaw").as("prospectivePhysicianRaw"),
      maraUdaf(MaraUtils.finalOrderingColumns.map(col):_*)("prospectiveTotalScoreRaw").as("prospectiveTotalScoreRaw"),
      maraUdaf(MaraUtils.finalOrderingColumns.map(col):_*)("prospectiveERScoreRaw").as("prospectiveERScoreRaw"),
      maraUdaf(MaraUtils.finalOrderingColumns.map(col):_*)("prospectiveOtherScoreRaw").as("prospectiveOtherScoreRaw"),
      maraUdaf(MaraUtils.finalOrderingColumns.map(col):_*)("concurrentInpatientRaw").as("concurrentInpatientRaw"),
      maraUdaf(MaraUtils.finalOrderingColumns.map(col):_*)("concurrentOutpatientRaw").as("concurrentOutpatientRaw"),
      maraUdaf(MaraUtils.finalOrderingColumns.map(col):_*)("concurrentMedicalRaw").as("concurrentMedicalRaw"),
      maraUdaf(MaraUtils.finalOrderingColumns.map(col):_*)("concurrentPharmacyRaw").as("concurrentPharmacyRaw"),
      maraUdaf(MaraUtils.finalOrderingColumns.map(col):_*)("concurrentPhysicianRaw").as("concurrentPhysicianRaw"),
      maraUdaf(MaraUtils.finalOrderingColumns.map(col):_*)("concurrentTotalScoreRaw").as("concurrentTotalScoreRaw"),
      maraUdaf(MaraUtils.finalOrderingColumns.map(col):_*)("concurrentERScoreRaw").as("concurrentERScoreRaw"),
      maraUdaf(MaraUtils.finalOrderingColumns.map(col):_*)("concurrentOtherScoreRaw").as("concurrentOtherScoreRaw"),
      maraUdaf(MaraUtils.finalOrderingColumns.map(col):_*)("conditionList").as("conditionList"),
      maraUdaf(MaraUtils.finalOrderingColumns.map(col):_*)("groupWiseAmounts").as("groupWiseAmounts"),
      maraUdaf(MaraUtils.finalOrderingColumns.map(col):_*)("totalPaidAmount").as("totalPaidAmount"),
      maraUdaf(MaraUtils.finalOrderingColumns.map(col):_*)("totalAllowedAmount").as("totalAllowedAmount"))

    combined
  }
}