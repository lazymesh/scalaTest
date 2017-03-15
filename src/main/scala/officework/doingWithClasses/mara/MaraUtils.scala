package officework.doingWithClasses.mara

import java.io.IOException
import java.util

import main.scala.officework.doingWithObjects.DateUtils
import milliman.mara.model.InputMedClaim
import org.apache.spark.sql.Row

/**
  * Created by ramaharjan on 3/13/17.
  */
object MaraUtils {

  var endOfCycleDate : String = _
  val FUTURE_DATE = DateUtils.convertStringToLong("2099-12-31")

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

  def getMemberFullName(input: Row): String = {
    val firstName = input.getString(MaraUtils.finalOrderingColumns.indexOf("mbr_first_name"))
    val middleName = input.getString(MaraUtils.finalOrderingColumns.indexOf("mbr_middle_name"))
    val lastName = input.getString(MaraUtils.finalOrderingColumns.indexOf("mbr_last_name"))
    lastName + ", " + firstName + (if (!middleName.isEmpty) " " + middleName else "")
  }

  def isClaimWithinTwelveMonths(serviceDate: Long, dataPeriodStartDate: Long, dataPeriodEndDate: Long): Boolean = serviceDate > dataPeriodStartDate && serviceDate <= dataPeriodEndDate

  def getInputMedClaim(claimLine: Row): String = {
    var inputMedClaim = ""
    inputMedClaim = inputMedClaim + claimLine.getString(MaraUtils.finalOrderingColumns.indexOf("dw_member_id")) + "::"+
      claimLine.getString(MaraUtils.finalOrderingColumns.indexOf("rev_claim_id")) + "::"+
      claimLine.getString(MaraUtils.finalOrderingColumns.indexOf("rev_claim_line_id")) + "::"+
      claimLine.getString(MaraUtils.finalOrderingColumns.indexOf("svc_service_frm_date")) + "::"+
      claimLine.getString(MaraUtils.finalOrderingColumns.indexOf("svc_service_to_date")) + "::"+
      claimLine.getString(MaraUtils.finalOrderingColumns.indexOf("rev_paid_date")) + "::"+
      claimLine.getString(MaraUtils.finalOrderingColumns.indexOf("svc_drg_code")) + "::"+
      claimLine.getString(MaraUtils.finalOrderingColumns.indexOf("svc_rev_code")) + "::"+
      claimLine.getString(MaraUtils.finalOrderingColumns.indexOf("prv_service_provider_id")) + "::"+
      claimLine.getString(MaraUtils.finalOrderingColumns.indexOf("svc_pos_code")) + "::"+
      claimLine.getString(MaraUtils.finalOrderingColumns.indexOf("rev_billed_amt")) + "::"+
      claimLine.getString(MaraUtils.finalOrderingColumns.indexOf("rev_paid_amt")) + "::"+
      claimLine.getString(MaraUtils.finalOrderingColumns.indexOf("rev_allowed_amt")) + "::"
      for (i <- 1 to 9) {
        inputMedClaim = inputMedClaim + claimLine.getString(MaraUtils.finalOrderingColumns.indexOf("svc_diag_" + i + "_code")) + "::"
      }
      val cptCode = claimLine.getString(MaraUtils.finalOrderingColumns.indexOf("svc_cpt_code"))
      val hcpcsCode = claimLine.getString(MaraUtils.finalOrderingColumns.indexOf("svc_hcpcs_code"))
      val icdCode = claimLine.getString(MaraUtils.finalOrderingColumns.indexOf("svc_icd_proc_1_code"))
      val procedureCode = if(!cptCode.isEmpty) cptCode else if (!hcpcsCode.isEmpty) hcpcsCode else if(!icdCode.isEmpty) icdCode else ""
      inputMedClaim = inputMedClaim + claimLine.getString(MaraUtils.finalOrderingColumns.indexOf("svc_pos_code")) + "::"+
        procedureCode

    inputMedClaim
  }

  def getInputRxClaim(rxLine: Row): String = {
    var inputRxClaim = ""
    inputRxClaim = inputRxClaim + rxLine.getString(MaraUtils.finalOrderingColumns.indexOf("dw_member_id")) + "::"
    inputRxClaim = inputRxClaim + rxLine.getString(MaraUtils.finalOrderingColumns.indexOf("svc_ndc_code")) + "::"
    inputRxClaim = inputRxClaim + rxLine.getString(MaraUtils.finalOrderingColumns.indexOf("rev_transaction_num")) + "::"
    inputRxClaim = inputRxClaim + rxLine.getString(MaraUtils.finalOrderingColumns.indexOf("rx_svc_filled_date")) + "::"
    inputRxClaim = inputRxClaim + rxLine.getString(MaraUtils.finalOrderingColumns.indexOf("prv_prescriber_id")) + "::"
    inputRxClaim = inputRxClaim + rxLine.getString(MaraUtils.finalOrderingColumns.indexOf("rev_billed_amt")) + "::"
    inputRxClaim = inputRxClaim + rxLine.getString(MaraUtils.finalOrderingColumns.indexOf("rev_paid_amt")) + "::"
    inputRxClaim = inputRxClaim + rxLine.getString(MaraUtils.finalOrderingColumns.indexOf("svc_days_of_supply")) + "::"
    inputRxClaim = inputRxClaim + rxLine.getString(MaraUtils.finalOrderingColumns.indexOf("svc_unit_qty")) + "::"
    inputRxClaim = inputRxClaim + rxLine.getString(MaraUtils.finalOrderingColumns.indexOf("rev_allowed_amt"))
    inputRxClaim
  }

  def getMaraMedClaimObject(medicalArrayList : util.List[Nothing]) {
    val inputMedClaimList = new util.ArrayList[InputMedClaim]
    for(claim <- medicalArrayList) {
      println(":::::::::::::::: "+claim)
      /*val claimLine = claim.split("::")
      val inputMedClaim = new InputMedClaim
      inputMedClaim.setMemberId(claim.get(0))
      if (claimLine.getString("rev_claim_id") != null) inputMedClaim.setClaimId(setValue(claimLine.getString("rev_claim_id")))
      inputMedClaim.setClaimSeq("" + setValue(claimLine.getString("rev_claim_line_id")))
      inputMedClaim.setFromDate(new Date(claimLine.getObject("svc_service_frm_date").asInstanceOf[Long]))
      inputMedClaim.setToDate(new Date(claimLine.getObject("svc_service_to_date").asInstanceOf[Long]))
      inputMedClaim.setPaidDate(new Date(claimLine.getObject("rev_paid_date").asInstanceOf[Long]))
      inputMedClaim.setDrg(setValue(claimLine.getString("svc_drg_code")))
      inputMedClaim.setRevCode(setValue(claimLine.getString("svc_rev_code")))
      inputMedClaim.setDrgVersion("")
      inputMedClaim.setSpecialty("")
      inputMedClaim.setProviderId(setValue(claimLine.getString("prv_service_provider_id")))
      inputMedClaim.setPos(setValue(claimLine.getString("svc_pos_code")))
      val billed_amount = if (StringUtils.isNull(claimLine.getString("rev_billed_amt"))) 0D
      else claimLine.getDouble("rev_billed_amt")
      val paid_amount = if (StringUtils.isNull(claimLine.getString("rev_paid_amt"))) 0D
      else claimLine.getDouble("rev_paid_amt")
      inputMedClaim.setCharged(billed_amount)
      inputMedClaim.setPaid(paid_amount)
      if (!StringUtils.isNull(claimLine.getString("rev_allowed_amt")) && claimLine.getDouble("rev_allowed_amt") > 0D) inputMedClaim.setAllowed(claimLine.getDouble("rev_allowed_amt"))
      else inputMedClaim.setAllowed(paid_amount)
      val diagList = new util.ArrayList[String]
      var i = 1
      while (i <= 9) {
        {
          diagList.add(setValue(claimLine.getString("svc_diag_" + i + "_code")))
        }
        {
          i += 1;
          i - 1
        }
      }
      inputMedClaim.setDiagList(diagList)
      var procedureCode = null
      val cptCode = claimLine.getString("svc_cpt_code")
      val hcpcsCode = claimLine.getString("svc_hcpcs_code")
      val icdCode = claimLine.getString("svc_icd_proc_1_code")
      if (!StringUtils.isNull(cptCode)) procedureCode = cptCode
      else if (!StringUtils.isNull(hcpcsCode)) procedureCode = hcpcsCode
      else if (!StringUtils.isNull(icdCode)) procedureCode = icdCode
      else procedureCode = ""
      inputMedClaim.setProcCode(setValue(procedureCode))*/
//      inputMedClaim
    }
  }
/*
  def getInputRxClaim(rxLine: TupleEntry): InputRxClaim = {
    val inputRxClaim = new InputRxClaim
    inputRxClaim.setMemberId(rxLine.getString("dw_member_id"))
    inputRxClaim.setNdcCode(rxLine.getString("svc_ndc_code"))
    inputRxClaim.setClaimId(rxLine.getString("rev_transaction_num"))
    inputRxClaim.setFillDate(new Date(rxLine.getObject("rx_svc_filled_date").asInstanceOf[Long]))
    inputRxClaim.setProviderId(rxLine.getString("prv_prescriber_id"))
    val billed_amount = if (StringUtils.isNull(rxLine.getString("rev_billed_amt"))) 0D
    else rxLine.getDouble("rev_billed_amt")
    val paid_amount = if (StringUtils.isNull(rxLine.getString("rev_paid_amt"))) 0D
    else rxLine.getDouble("rev_paid_amt")
    val daysOfSupply = if (StringUtils.isNull(rxLine.getString("svc_days_of_supply"))) 0
    else rxLine.getInteger("svc_days_of_supply")
    val svcQuantity = if (StringUtils.isNull(rxLine.getString("svc_unit_qty"))) 0
    else rxLine.getInteger("svc_unit_qty")
    inputRxClaim.setCharged(billed_amount)
    inputRxClaim.setPaid(paid_amount)
    inputRxClaim.setDaysSupplied(daysOfSupply)
    inputRxClaim.setQtyDispensed(svcQuantity)
    if (!StringUtils.isNull(rxLine.getString("rev_allowed_amt")) && rxLine.getDouble("rev_allowed_amt") > 0D) inputRxClaim.setAllowed(rxLine.getDouble("rev_allowed_amt"))
    else inputRxClaim.setAllowed(paid_amount)
    inputRxClaim
  }*/
}
