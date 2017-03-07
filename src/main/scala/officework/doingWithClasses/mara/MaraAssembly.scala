package main.scala.officework.doingWithClasses.mara

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lit, row_number, udf}

/**
  * Created by ramaharjan on 3/2/17.
  */
class MaraAssembly(eligDataFrame : DataFrame, medDataFrame : DataFrame, rxDataFrame : DataFrame) {

  var eligDF = eligDataFrame
  var medDF = medDataFrame
  var rxDF = rxDataFrame

  val commonColumns = Vector("dw_member_id", "ins_emp_group_id", "ins_emp_group_name", "ins_division_id", "ins_division_name", "ins_carrier_id", "ins_carrier_name", "ins_plan_id", "ins_plan_type_code", "udf16", "udf17", "udf18", "udf19", "udf20", "udf21", "udf22", "udf23", "udf24", "udf25")

  val commonMedRxColumns = Vector("rev_billed_amt", "rev_paid_amt", "rev_allowed_amt")
  val commonMedRxValues = Vector("billed_amt", "paid_amt", "allowed_amt")

  val eligOnlyColumns = Vector("mbr_dob", "ins_med_eff_date", "ins_med_term_date", "mbr_gender", "mbr_relationship_code", "mbr_relationship_desc", "mbr_current_status", "mbr_id", "mbr_first_name", "mbr_middle_name", "mbr_last_name", "integer_member_id")
  val eligOnlyValues = Vector("1990-01-01", "1990-01-01", "1990-01-01", "gender", "relation_code", "relation_desc", "status", "unBlinded_id", "first_name", "middle_name", "last_name", 0)

  val medOnlyColumns = Vector("svc_service_frm_date", "svc_service_to_date", "rev_paid_date", "rev_claim_id", "rev_claim_line_id", "prv_service_provider_id", "svc_cpt_code", "svc_pos_code", "svc_drg_code", "svc_rev_code", "svc_procedure_code", "svc_hcpcs_code", "svc_icd_proc_1_code", "svc_diag_1_code", "svc_diag_2_code", "svc_diag_3_code", "svc_diag_4_code", "svc_diag_5_code", "svc_diag_6_code", "svc_diag_7_code", "svc_diag_8_code", "svc_diag_9_code")
  val medOnlyValues = Vector("1990-01-01", "1990-01-01", "1990-01-01", "claim_id", "claim_line_id", "provider_id", "cpt", "pos", "drg", "rev", "proc_code", "hcpcs", "icd_proc_1_code", "diag_1", "diag_2", "diag_3", "diag_4", "diag_5", "diag_6", "diag_7", "diag_8", "diag_9")

  val rxOnlyColumns = Vector("rx_svc_filled_date", "rev_transaction_num", "svc_ndc_code", "svc_days_of_supply", "prv_prescriber_id", "svc_unit_qty")
  val rxOnlyValues = Vector("1990-01-01", "claim_id", "ndc", "dos", "prescriber_id", "qty")

  val inputTypeFlagColumn = Vector("inputTypeFlag")
  val inputTypeFlagEligLatest = Vector(3)
  val inputTypeFlagElig = Vector(2)
  val inputTypeFlagRx = Vector(1)
  val inputTypeFlagMed = Vector(0)

  val sortDateColumn : String = "sortDate"
  val sortDate = Vector("sortDate")

  // Eligibility fields
  val eligRetain = commonColumns ++ eligOnlyColumns
  val insertToEligColumns = medOnlyColumns ++ rxOnlyColumns ++ commonMedRxColumns ++ inputTypeFlagColumn
  val insertToEligValues = medOnlyValues ++ rxOnlyValues ++ commonMedRxValues ++ inputTypeFlagElig

  // Medical fields
  val medRetain = commonColumns ++ commonMedRxColumns ++ medOnlyColumns
  val insertToMedColumns = rxOnlyColumns ++ eligOnlyColumns ++ inputTypeFlagColumn
  val insertToMedValues = rxOnlyValues ++ eligOnlyValues ++ inputTypeFlagMed

  // Rx fields
  val rxRetain = commonColumns ++ commonMedRxColumns ++ rxOnlyColumns
  val insertToRxColumns = medOnlyColumns ++ eligOnlyColumns ++ inputTypeFlagColumn
  val insertToRxValues = medOnlyValues ++ eligOnlyValues ++ inputTypeFlagRx

  val finalOrderingColumns = commonColumns ++ eligOnlyColumns ++ medOnlyColumns ++ commonMedRxColumns ++ rxOnlyColumns ++ inputTypeFlagColumn ++ sortDate

  def setSortDate = udf((date: String) => date )

  def maraCalculator() : Unit = {

    //todo groupfilter of elig
    eligDF = eligDF.select(eligRetain.map(col): _*)
    for (column <- 0 until insertToEligColumns.size) {
      eligDF = eligDF.withColumn(insertToEligColumns(column), lit(insertToEligValues(column)))
    }
    eligDF = eligDF.withColumn(sortDateColumn, setSortDate(eligDF("ins_med_term_date")))
    eligDF = eligDF.select(finalOrderingColumns.map(col):_*)

    val window = Window.partitionBy(col("dw_member_id")).orderBy(col("ins_med_term_date").desc, col("ins_med_eff_date").desc, col("mbr_relationship_code").desc, col("ins_emp_group_id").desc)
    val latestEligDF = eligDF.withColumn("rn", row_number.over(window)).where(col("rn") === 1).drop(col("rn")).withColumn(inputTypeFlagColumn(0), lit(inputTypeFlagEligLatest(0)))

    //todo groupfilter of medical
    medDF = medDF.select(medRetain.map(col): _*)
    for (column <- 0 until insertToMedColumns.size) {
      medDF = medDF.withColumn(insertToMedColumns(column), lit(insertToMedValues(column)))
    }
    medDF = medDF.withColumn(sortDateColumn, setSortDate(medDF("svc_service_frm_date")))
    //todo convert "svc_service_frm_date", "svc_service_to_date", "rev_paid_date", "ins_med_eff_date", "ins_med_term_date" to date
    medDF = medDF.select(finalOrderingColumns.map(col):_*)

    //todo groupfilter of pharmacy
    rxDF = rxDF.withColumnRenamed("svc_service_frm_date", "rx_svc_filled_date")
    rxDF = rxDF.select(rxRetain.map(col): _*)
    for (column <- 0 until insertToRxColumns.size) {
      rxDF = rxDF.withColumn(insertToRxColumns(column), lit(insertToRxValues(column)))
    }
    rxDF = rxDF.withColumn(sortDateColumn, setSortDate(rxDF("rx_svc_filled_date")))
    //todo convert "rx_svc_filled_date","ins_med_eff_date", "ins_med_term_date" to date
    rxDF = rxDF.select(finalOrderingColumns.map(col):_*)

    var combined = latestEligDF.union(eligDF).union(medDF).union(rxDF)



  }
}