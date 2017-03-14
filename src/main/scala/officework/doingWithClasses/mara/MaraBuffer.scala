package officework.doingWithClasses.mara

import java.util
import java.util.Date

import main.scala.officework.doingWithObjects.DateUtils
import milliman.mara.model.InputRxClaim
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.types.StructField
import org.joda.time.DateTime

/**
  * Created by ramaharjan on 3/13/17.
  */
class MaraBuffer {

  var currentCycleEndDate = DateUtils.convertStringToLong(MaraUtils.endOfCycleDate)
  var dataPeriodStartDate = new DateTime(currentCycleEndDate).minusMonths(12).getMillis
  val eligibleDateRanges = new util.TreeMap[Long, Long]
  val inputRxClaimList = new util.ArrayList[String]

  def populate(buffer : MutableAggregationBuffer, input : Row): Unit ={
    val inputTypeFlag = input.getInt(MaraUtils.finalOrderingColumns.indexOf("inputTypeFlag"))
    println(input.getString(0)+" OOOOOOOOOOOOOOOOOOOOOOOO "+inputTypeFlag)
    val memberFields : util.ArrayList[String] = new util.ArrayList[String]
    if (inputTypeFlag == (MaraUtils.inputTypeFlagEligLatest(0))) {
      //setting demographics from the latest eligibility record.
      val mbrDob = input.getString(MaraUtils.finalOrderingColumns.indexOf("mbr_dob"))
      memberFields.add(mbrDob)
      val relationshipCode = input.getString(MaraUtils.finalOrderingColumns.indexOf("mbr_relationship_code"))
      memberFields.add(relationshipCode)
      val relationshipDesc = input.getString(MaraUtils.finalOrderingColumns.indexOf("mbr_relationship_desc"))
      memberFields.add(relationshipDesc)
      val gender = input.getString(MaraUtils.finalOrderingColumns.indexOf("mbr_gender"))
      memberFields.add(gender)
      val unblindMemberId = input.getString(MaraUtils.finalOrderingColumns.indexOf("mbr_id"))
      memberFields.add(unblindMemberId)
      val status = input.getString(MaraUtils.finalOrderingColumns.indexOf("mbr_current_status"))
      memberFields.add(status)
      val memberFullName = MaraUtils.getMemberFullName(input)
      memberFields.add(memberFullName)
      val mbrGroupId = input.getString(MaraUtils.finalOrderingColumns.indexOf("ins_emp_group_id"))
      memberFields.add(mbrGroupId)
      val mbrGroupName = input.getString(MaraUtils.finalOrderingColumns.indexOf("ins_emp_group_name"))
      memberFields.add(mbrGroupName)
      val ins_division_id = input.getString(MaraUtils.finalOrderingColumns.indexOf("ins_division_id"))
      memberFields.add(ins_division_id)
      val ins_carrier_id = input.getString(MaraUtils.finalOrderingColumns.indexOf("ins_carrier_id"))
      memberFields.add(ins_carrier_id)
      val ins_plan_id = input.getString(MaraUtils.finalOrderingColumns.indexOf("ins_plan_id"))
      memberFields.add(ins_plan_id)
      val udf16 = input.getString(MaraUtils.finalOrderingColumns.indexOf("udf16"))
      memberFields.add(udf16)
      val udf17 = input.getString(MaraUtils.finalOrderingColumns.indexOf("udf17"))
      memberFields.add(udf17)
      val udf18 = input.getString(MaraUtils.finalOrderingColumns.indexOf("udf18"))
      memberFields.add(udf18)
      val udf19 = input.getString(MaraUtils.finalOrderingColumns.indexOf("udf19"))
      memberFields.add(udf19)
      val udf20 = input.getString(MaraUtils.finalOrderingColumns.indexOf("udf20"))
      memberFields.add(udf20)
      val udf21 = input.getString(MaraUtils.finalOrderingColumns.indexOf("udf21"))
      memberFields.add(udf21)
      val udf22 = input.getString(MaraUtils.finalOrderingColumns.indexOf("udf22"))
      memberFields.add(udf22)
      val udf23 = input.getString(MaraUtils.finalOrderingColumns.indexOf("udf23"))
      memberFields.add(udf23)
      val udf24 = input.getString(MaraUtils.finalOrderingColumns.indexOf("udf24"))
      memberFields.add(udf24)
      val udf25 = input.getString(MaraUtils.finalOrderingColumns.indexOf("udf25"))
      memberFields.add(udf25)
      val ins_plan_type_code = input.getString(MaraUtils.finalOrderingColumns.indexOf("ins_plan_type_code"))
      memberFields.add(ins_plan_type_code)
      val integer_member_id = input.getString(MaraUtils.finalOrderingColumns.indexOf("integer_member_id"))
      memberFields.add(integer_member_id)
      buffer.update(3, input.getString(MaraUtils.finalOrderingColumns.indexOf("dw_member_id")))

      //if the client contains groupwise processing then end cycle dates varies according to groups
/*      if (this.groupWiseProcessing.equalsIgnoreCase("groupWiseProcess")) {
        grouped_End_Cycle_Dates = entry.getString("cycleEndDate")
        setGroupWiseProcessing(entry)
      }
      context.modelProcessor.modelProcessorMARA.setEndModelPeriodDate(new Date(endOfCycleDate))*/
      buffer.update(0, memberFields)
    }
    else if (inputTypeFlag == (MaraUtils.inputTypeFlagElig(0))) {
      //inputFlagType --> 2
      var effDate = DateUtils.convertStringToLong(input.getString(MaraUtils.finalOrderingColumns.indexOf("ins_med_eff_date")))
      var termDate = DateUtils.convertStringToLong(input.getString(MaraUtils.finalOrderingColumns.indexOf("ins_med_term_date")))

/*      if (eligibleDate.equalsIgnoreCase("increase")) {
        effDate = DateUtils.getIncreaseEligibleFromDate(effDate).getMillis
        termDate = DateUtils.getMaxEligibleToDate(termDate).getMillis
      }
      else termDate = DateUtils.getEligibleToDate(termDate).getMillis
      val currentCycleEndDate = new DateTime(endOfCycleDate).plusMonths(decrease_month).dayOfMonth.withMaximumValue.getMillis*/
      if (currentCycleEndDate > effDate && currentCycleEndDate <= termDate) {
        buffer.update(1, true)
        addEligibleDateRanges(eligibleDateRanges, effDate, termDate)
        buffer.update(2, eligibleDateRanges)
      }
      //adding eligible date ranges
      //todo summable maps
/*      paidAmount.put(entry.getString("ins_emp_group_id"), 0D)
      allowedAmount.put(entry.getString("ins_emp_group_id"), 0D)*/
    }
    else if (inputTypeFlag == (MaraUtils.inputTypeFlagRx(0))) {
      //inputFlagType --> 1
      val serviceDate = DateUtils.convertStringToLong(input.getString(MaraUtils.finalOrderingColumns.indexOf("rx_svc_filled_date")))
/*      val paid_amount = if (StringUtils.isNull(entry.getString("rev_paid_amt"))) 0D
      else entry.getDouble("rev_paid_amt")
      val allowed_amount = if (StringUtils.isNull(entry.getString("rev_allowed_amt"))) 0D
      else entry.getDouble("rev_allowed_amt")*/
      println(input.getString(MaraUtils.finalOrderingColumns.indexOf("dw_member_id")) +" "+input.getString(MaraUtils.finalOrderingColumns.indexOf("rx_svc_filled_date"))+" UUUUUUUUUUUUUUUUUUUUUUUUUUUUU "+MaraUtils.isClaimWithinTwelveMonths(serviceDate, dataPeriodStartDate, currentCycleEndDate))
      if (MaraUtils.isClaimWithinTwelveMonths(serviceDate, dataPeriodStartDate, currentCycleEndDate)) {
//        totalRxPaid += paid_amount
//        totalRxAllowedAmt += allowed_amount
        //        paidAmount.put(entry.getString("ins_emp_group_id"), paid_amount)
        //        allowedAmount.put(entry.getString("ins_emp_group_id"), allowed_amount)
        println("KKKKKKKKKKKKKKKK "+MaraUtils.getInputRxClaim(input))
//        buffer.update(4, inputRxClaimList.add(MaraUtils.getInputRxClaim(input)))
      }
    }
/*    else if (inputTypeFlag.matches(MaraUtils.INPUT_TYPE_Medical)) {
      //inputFlagType --> 0
      if (!isMemberActive) {
        //no need to continue if member is not active
        System.out.println("Member " + entry.getString("dw_member_id") + " is inactive with no pharmacy claims")
        break //todo: break is not supported
      }
      val serviceDate = entry.getObject("svc_service_frm_date").asInstanceOf[Long]
      val paid_amount = if (StringUtils.isNull(entry.getString("rev_paid_amt"))) 0D
      else entry.getDouble("rev_paid_amt")
      val allowed_amount = if (StringUtils.isNull(entry.getString("rev_allowed_amt"))) 0D
      else entry.getDouble("rev_allowed_amt")
      if (MaraUtils.isClaimWithinTwelveMonths(serviceDate, dataPeriodStartDate, dataPeriodEndDate)) try
        totalMedPaid += paid_amount
        totalMedAllowedAmt += allowed_amount
        inputMedClaimList.add(MaraUtils.getInputMedClaim(entry))
        paidAmount.put(entry.getString("ins_emp_group_id"), paid_amount)
        allowedAmount.put(entry.getString("ins_emp_group_id"), allowed_amount)

      catch {
        case e: IOException => {
          System.out.println("IO Exception has occurred ??????")
          e.printStackTrace()
          throw new RuntimeException("IO exception for member " + entry.getString("dw_member_id"))
        }
      }
    }*/
  }

  private def addEligibleDateRanges(eligibleDateRanges: util.TreeMap[Long, Long], effectiveDate: Long, terminationDate: Long) {
    var effDate = effectiveDate
    var termDate = terminationDate
    if (effDate < termDate) {
      if (termDate == MaraUtils.FUTURE_DATE) termDate = currentCycleEndDate
      if (termDate > currentCycleEndDate) termDate = currentCycleEndDate
      if (effDate < dataPeriodStartDate) effDate = dataPeriodStartDate

      if (!eligibleDateRanges.isEmpty && eligibleDateRanges.containsKey(effectiveDate) && (eligibleDateRanges.get(effectiveDate) > terminationDate))
        eligibleDateRanges.put(effectiveDate, eligibleDateRanges.get(effectiveDate))
      else
        eligibleDateRanges.put(effectiveDate, terminationDate)
    }
  }

}
