package main.scala.officework.doingWithClasses.mara

/**
  * Created by ramaharjan on 3/2/17.
  */
class MaraAssembly {
/*

  var fsCommon_All = {"dw_member_id", "ins_emp_group_id", "ins_emp_group_name", "ins_division_id", "ins_division_name", "ins_carrier_id", "ins_carrier_name", "ins_plan_id", "ins_plan_type_code", "udf16", "udf17", "udf18", "udf19", "udf20", "udf21", "udf22", "udf23", "udf24", "udf25"}
  var fsCommon_Med_Rx = {"rev_billed_amt", "rev_paid_amt", "rev_allowed_amt"}
  var fsElig_Only = {"mbr_dob", "ins_med_eff_date", "ins_med_term_date", "mbr_gender", "mbr_relationship_code", "mbr_relationship_desc", "mbr_current_status", "mbr_id", "mbr_first_name", "mbr_middle_name", "mbr_last_name", "integer_member_id"}
  var fsMed_Only = {"svc_service_frm_date", "svc_service_to_date", "rev_paid_date", "rev_claim_id", "rev_claim_line_id", "prv_service_provider_id", "svc_cpt_code", "svc_pos_code", "svc_drg_code", "svc_rev_code", "svc_procedure_code", "svc_hcpcs_code", "svc_icd_proc_1_code", "svc_diag_1_code", "svc_diag_2_code", "svc_diag_3_code", "svc_diag_4_code", "svc_diag_5_code", "svc_diag_6_code", "svc_diag_7_code", "svc_diag_8_code", "svc_diag_9_code"}
  var fsRx_Only = {"rx_svc_filled_date", "rev_transaction_num", "svc_ndc_code", "svc_days_of_supply", "prv_prescriber_id", "svc_unit_qty"}
  // Eligibility fields
  var fsEligRetain: String = fsCommon_All + fsElig_Only
  // Medical fields
  var fsMedRetain: String = fsCommon_All + fsCommon_Med_Rx + fsMed_Only
  // Rx fields
  var fsRxRetain: String = fsCommon_All + fsCommon_Med_Rx + fsRx_Only
  // rx flag
  var fsInputTypeFlag = "inputTypeFlag"
  // date sort field
  var fsSortDate = "sortDate"
  // final fields
  var fsFinal: Fields = fsCommon_All.append(fsCommon_Med_Rx).append(fsMed_Only).append(fsRx_Only).append(fsElig_Only).append(fsSortDate).append(fsInputTypeFlag)
  private[assembly] var s_medLimit = NO_LIMIT
  private[assembly] var s_rxLimit = NO_LIMIT
  private[assembly] var s_eligLimit = NO_LIMIT
  private[assembly] var s_testLimit = NO_LIMIT
  private[assembly] var s_testPipe = null

  def SetLimits(med: Long, rx: Long) {
    s_medLimit = med
    s_rxLimit = rx
  }

  def SetLimits(med: Long, rx: Long, test: Long) {
    s_medLimit = med
    s_rxLimit = rx
    s_eligLimit = test
  }

  def SetTestPipe(testPipe: Pipe, testLimit: Long) {
    s_testPipe = testPipe
    s_testLimit = testLimit
  }

  def SetTestPipe(testPipe: Pipe) {
    s_testPipe = testPipe
    s_testLimit = NO_LIMIT
  }

  def this(eligSrcPipe: Pipe, medSrcPipe: Pipe, rxSrcPipe: Pipe, groupWisePipe: Pipe, SOC: String, EOC: String, groupToExclude: String, groupWiseProcess: String, maraMonth: String, url: Array[String], maraCycleDateChangeInMonth: Int) {
    this(eligSrcPipe, medSrcPipe, rxSrcPipe, groupWisePipe, SOC, EOC, groupToExclude, false, groupWiseProcess, maraMonth, url, maraCycleDateChangeInMonth)
  }

  def this(eligSrcPipe: Pipe, medSrcPipe: Pipe, rxSrcPipe: Pipe, groupWisePipe: Pipe, SOC: String, EOC: String, groupToExclude: String, setTestEnv: Boolean, groupWiseProcess: String, maraMonth: String, url: Array[String], maraCycleDateChangeInMonth: Int)
*/

}