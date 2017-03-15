package officework.doingWithClasses.mara

import java.util
import java.util.Date

import main.scala.officework.doingWithObjects.DateUtils
import milliman.mara.exception.{MARAEngineProcessException, MARAInvalidMemberException}
import milliman.mara.model._
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.joda.time.{DateTime, Months, Years}

/**
  * Created by ramaharjan on 3/13/17.
  */
class MaraBuffer {

  var currentCycleEndDate = DateUtils.convertStringToLong(MaraUtils.endOfCycleDate)
  var dataPeriodStartDate = new DateTime(currentCycleEndDate).minusMonths(12).getMillis

  val dataPeriodMonthYears = new util.ArrayList[String](12)
  val eocDateTime = new DateTime(currentCycleEndDate)
  dataPeriodMonthYears.clear()
  for(i <- 1 to 12){
    val newDate = eocDateTime.minusMonths(i)
    dataPeriodMonthYears.add(newDate.getMonthOfYear + "-" + newDate.getYear)
  }

  val eligibleDateRanges = new util.TreeMap[Long, Long]
  val inputRxClaimList = new util.ArrayList[String]
  val inputMedClaimList = new util.ArrayList[String]

  def populate(buffer : MutableAggregationBuffer, input : Row): Unit = {
    val inputTypeFlag = input.getInt(MaraUtils.finalOrderingColumns.indexOf("inputTypeFlag"))
    val memberFields: util.ArrayList[String] = new util.ArrayList[String]
    if (inputTypeFlag == (MaraUtils.inputTypeFlagEligLatest(0))) {
      //setting demographics from the latest eligibility record.
      val memberId = input.getString(MaraUtils.finalOrderingColumns.indexOf("dw_member_id"))
      memberFields.add(memberId)
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
      //      println(input.getString(MaraUtils.finalOrderingColumns.indexOf("dw_member_id")) +" "+input.getString(MaraUtils.finalOrderingColumns.indexOf("rx_svc_filled_date"))+" UUUUUUUUUUUUUUUUUUUUUUUUUUUUU "+MaraUtils.isClaimWithinTwelveMonths(serviceDate, dataPeriodStartDate, currentCycleEndDate))
      if (MaraUtils.isClaimWithinTwelveMonths(serviceDate, dataPeriodStartDate, currentCycleEndDate)) {
        //        totalRxPaid += paid_amount
        //        totalRxAllowedAmt += allowed_amount
        //        paidAmount.put(entry.getString("ins_emp_group_id"), paid_amount)
        //        allowedAmount.put(entry.getString("ins_emp_group_id"), allowed_amount)
        inputRxClaimList.add(MaraUtils.getInputRxClaim(input))
        buffer.update(3, inputRxClaimList)
      }
    }
    else if (inputTypeFlag == (MaraUtils.inputTypeFlagMed(0))) {
      //inputFlagType --> 0
      val serviceDate = DateUtils.convertStringToLong(input.getString(MaraUtils.finalOrderingColumns.indexOf("svc_service_frm_date")))
      /*      val paid_amount = if (StringUtils.isNull(entry.getString("rev_paid_amt"))) 0D
      else entry.getDouble("rev_paid_amt")
      val allowed_amount = if (StringUtils.isNull(entry.getString("rev_allowed_amt"))) 0D
      else entry.getDouble("rev_allowed_amt")*/
      if (MaraUtils.isClaimWithinTwelveMonths(serviceDate, dataPeriodStartDate, currentCycleEndDate)) {
        /*        totalMedPaid += paid_amount
        totalMedAllowedAmt += allowed_amount
        paidAmount.put(entry.getString("ins_emp_group_id"), paid_amount)
        allowedAmount.put(entry.getString("ins_emp_group_id"), allowed_amount)*/
        inputMedClaimList.add(MaraUtils.getInputMedClaim(input))
        buffer.update(4, inputMedClaimList)
      }
    }
  }

  def calculateMaraScores(buffer : Row, modelProcessor: ModelProcessor) {
    val member = new InputMember
    val memberInfo = buffer.getList(0)
    member.setMemberId(memberInfo.get(0))
    member.setDob(new Date(DateUtils.convertStringToLong(memberInfo.get(1))))
    member.setDependentStatus(MaraUtils.getDependentStatus(memberInfo.get(2)))
    member.setGender(MaraUtils.getGender(memberInfo.get(4)))
    member.setExposureMonths(calculateExposureMonths(buffer.getMap(2).asInstanceOf[Map[Long, Long]]))
    member.setInputMedClaim(MaraUtils.getMaraMedClaimObject(buffer.getList(4).toArray()))
    member.setInputRxClaim(MaraUtils.getMaraRxClaimObject(buffer.getList(3).toArray()))
    val maraOutputScores = calculateRiskScores(member, modelProcessor)

    val modelDataMap = maraOutputScores.getOutputModelDataMap
    val prospectiveModelScore = modelDataMap.get("CXPROLAG0").getOutputModelScore
    val concurrentModelScore = modelDataMap.get("CXCONLAG0").getOutputModelScore
    val conditionMap = modelDataMap.get("CXPROLAG0").getOutputPercentContribution
    println("GGGGGGGGGGGGGGGGGGGGGGGG ")
    println(prospectiveModelScore.getHipScore)
    println(prospectiveModelScore.getHopScore)
    println(prospectiveModelScore.getMedScore)
    println(prospectiveModelScore.getRxScore)
    println(prospectiveModelScore.getPhyScore)
    println(prospectiveModelScore.getTotScore)
    println(prospectiveModelScore.getErScore)
    println(prospectiveModelScore.getOthScore)
    println(concurrentModelScore.getHipScore)
    println(concurrentModelScore.getHopScore)
    println(concurrentModelScore.getMedScore)
    println(concurrentModelScore.getRxScore)
    println(concurrentModelScore.getPhyScore)
    println(concurrentModelScore.getTotScore)
    println(concurrentModelScore.getErScore)
    println(concurrentModelScore.getOthScore)

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

  private def calculateExposureMonths(eligibleDateRanges: collection.Map[Long, Long]) = {
    val eligibleMonths = new util.HashSet[String]
    var exposureMonths = 0
    for(entry <- eligibleDateRanges){
      val fromDate = entry._1
      val toDate = entry._2
      var endDate = new DateTime(toDate)
      val months = Months.monthsBetween(new DateTime(fromDate), new DateTime(toDate)).getMonths
      for(i <- 1 to months) {
        eligibleMonths.add(endDate.getMonthOfYear + "-" + endDate.getYear)
        endDate = endDate.minusMonths(1)
      }
    }
    import scala.collection.JavaConverters._
    for(eligibleMonthYear <- eligibleMonths.asScala) {
      if (dataPeriodMonthYears.contains(eligibleMonthYear)) exposureMonths += 1
    }
    exposureMonths
  }

  private def calculateRiskScores(inputMember: InputMember, modelProcessor: ModelProcessor) = {
    var outputMaraResultSet : OutputMaraResultSet = new OutputMaraResultSet
    try {
      outputMaraResultSet = modelProcessor.processMember(inputMember)
    }
    catch {
      case e: MARAInvalidMemberException => {
        outputMaraResultSet = tryRecalculatingRiskScoreForInvalidMembers(inputMember, modelProcessor)
        if (outputMaraResultSet == null) {
          println("Warning mara scores not calculate for member " + inputMember.getMemberId)
          //throw new RuntimeException("Invalid Member : Dob " + inputMember.getDob());
          MaraUtils.debugMember(inputMember)
        }
        else outputMaraResultSet
      }
      case e: MARAEngineProcessException => {
        System.out.println("MaraEngineProcessException" + inputMember.getMemberId + " :: " + e.getMessage)
        //            saveLogs.setLogsAsString("MARAEngineProcessException "+inputMember.getMemberId(), e);
        throw new RuntimeException
      }
      case ne: NullPointerException => {
        System.out.println("Invalid users : " + inputMember.getMemberId)
        //            saveLogs.setLogsAsString("NullPointerException "+inputMember.getMemberId(), ne);
        System.out.println(ne.getLocalizedMessage)
        ne.printStackTrace(System.out)
      }
    }
    outputMaraResultSet
  }

  def tryRecalculatingRiskScoreForInvalidMembers(inputMember: InputMember, modelProcessor: ModelProcessor) = {
    val age = Years.yearsBetween(new DateTime(inputMember.getDob.getTime), new DateTime(MaraUtils.endOfCycleDate)).getYears
    System.out.println("Invalid Age " + age + " CycleEndDate " + new Date(MaraUtils.endOfCycleDate) + " Member Id " + inputMember.getMemberId)
    val cycleEndDate = new DateTime(MaraUtils.endOfCycleDate)
    val newDob = cycleEndDate.minusYears(35).getMillis
    inputMember.setDob(new Date(newDob))
    var outputMaraResultSet : OutputMaraResultSet = new OutputMaraResultSet
    try
      outputMaraResultSet = modelProcessor.processMember(inputMember)

    catch {
      case e: MARAInvalidMemberException => {
        System.out.println(e.getMessage)
        null
      }
      case e: MARAEngineProcessException => {
        System.out.println(e.getMessage)
        null
      }
    }
    outputMaraResultSet
  }
}
