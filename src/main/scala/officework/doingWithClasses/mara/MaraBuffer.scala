package officework.doingWithClasses.mara

import java.util
import java.util.Date

import main.scala.officework.doingWithClasses.DateUtility
import main.scala.officework.doingWithObjects.DateUtils
import milliman.mara.exception.{MARAEngineProcessException, MARAInvalidMemberException}
import milliman.mara.model.{InputMember, ModelProcessor, OutputMaraResultSet}
import officework.doingWithClasses.{StringUtility, SummableMap}
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.joda.time.{DateTime, Months, Years}

import scala.collection.mutable

/**
  * Created by ramaharjan on 3/13/17.
  */
class MaraBuffer() extends Serializable {

  val conditionMapFromFile = MaraUtils.getConditionMap

  val dataPeriodMonthYears = new util.ArrayList[String](12)
  val eocDateTime = new DateTime(MaraUtils.clientConfigCycleEndDate)
  dataPeriodMonthYears.clear()
  for(i <- 1 to 12){
    val newDate = eocDateTime.minusMonths(i)
    dataPeriodMonthYears.add(newDate.getMonthOfYear + "-" + newDate.getYear)
  }

  val eligibleDateRanges = new util.TreeMap[Long, Long]
  val inputRxClaimList = new util.ArrayList[String]
  val inputMedClaimList = new util.ArrayList[String]

  val paidAmount : SummableMap[String, Double] = new SummableMap[String, Double]
  val allowedAmount : SummableMap[String, Double] = new SummableMap[String, Double]

  def initializeOrClear : Unit = {
    eligibleDateRanges.clear()
    inputMedClaimList.clear()
    inputRxClaimList.clear()
    paidAmount.clear()
    allowedAmount.clear()
  }

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
      val integer_member_id = input.getInt(MaraUtils.finalOrderingColumns.indexOf("integer_member_id")).toString
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
      var effDate = DateUtility.convertStringToLong(input.getString(MaraUtils.finalOrderingColumns.indexOf("ins_med_eff_date")))
      var termDate = DateUtility.convertStringToLong(input.getString(MaraUtils.finalOrderingColumns.indexOf("ins_med_term_date")))

      /*      if (eligibleDate.equalsIgnoreCase("increase")) {
        effDate = DateUtils.getIncreaseEligibleFromDate(effDate).getMillis
        termDate = DateUtils.getMaxEligibleToDate(termDate).getMillis
      }
      else termDate = DateUtils.getEligibleToDate(termDate).getMillis
      val currentCycleEndDate = new DateTime(endOfCycleDate).plusMonths(decrease_month).dayOfMonth.withMaximumValue.getMillis*/
      if (MaraUtils.clientConfigCycleEndDate > effDate && MaraUtils.clientConfigCycleEndDate <= termDate) {
        buffer.update(1, true)
        addEligibleDateRanges(eligibleDateRanges, effDate, termDate)
        paidAmount.put(input.getString(MaraUtils.finalOrderingColumns.indexOf("ins_emp_group_id")), 0D)
        allowedAmount.put(input.getString(MaraUtils.finalOrderingColumns.indexOf("ins_emp_group_id")), 0D)
        buffer.update(2, eligibleDateRanges)
        buffer.update(5, paidAmount)
        buffer.update(6, allowedAmount)
      }
    }
    else if (inputTypeFlag == (MaraUtils.inputTypeFlagRx(0))) {
      //inputFlagType --> 1
      val filledDate = if(StringUtility.isNotNull(input.getString(MaraUtils.finalOrderingColumns.indexOf("rx_svc_filled_date"))))
        input.getString(MaraUtils.finalOrderingColumns.indexOf("rx_svc_filled_date")) else "2099-12-31"
      //      val serviceDate = DateUtility.convertStringToLong(input.getString(MaraUtils.finalOrderingColumns.indexOf("rx_svc_filled_date")))
      val paid_amount = if (StringUtility.isNull(input.getString(MaraUtils.finalOrderingColumns.indexOf("rev_paid_amt")))) 0D
      else input.getString(MaraUtils.finalOrderingColumns.indexOf("rev_paid_amt")).toDouble
      val allowed_amount = if (StringUtility.isNull(input.getString(MaraUtils.finalOrderingColumns.indexOf("rev_allowed_amt")))) 0D
      else input.getString(MaraUtils.finalOrderingColumns.indexOf("rev_allowed_amt")).toDouble

      val memberId = input.getString(MaraUtils.finalOrderingColumns.indexOf("dw_member_id"))
      println(memberId+DateUtility.convertLongToString(MaraUtils.dataPeriodStartDate)+"::::::::::PHARMACY "+filledDate+":::::::: "+DateUtility.convertLongToString(MaraUtils.clientConfigCycleEndDate))
      if (MaraUtils.isClaimWithinTwelveMonths(DateUtility.convertStringToLong(filledDate), MaraUtils.dataPeriodStartDate, MaraUtils.clientConfigCycleEndDate)) {
        inputRxClaimList.add(MaraUtils.getInputRxClaim(input))
        paidAmount.put(input.getString(MaraUtils.finalOrderingColumns.indexOf("ins_emp_group_id")), paid_amount)
        allowedAmount.put(input.getString(MaraUtils.finalOrderingColumns.indexOf("ins_emp_group_id")), allowed_amount)
        buffer.update(3, inputRxClaimList)
        buffer.update(5, paidAmount)
        buffer.update(6, allowedAmount)
      }
    }
    else if (inputTypeFlag == (MaraUtils.inputTypeFlagMed(0))) {
      //inputFlagType --> 0
      var serviceFromDate = if(StringUtility.isNotNull(input.getString(MaraUtils.finalOrderingColumns.indexOf("svc_service_frm_date"))))
        input.getString(MaraUtils.finalOrderingColumns.indexOf("svc_service_frm_date")) else "2099-12-31"
      //      val serviceDate = DateUtility.convertStringToLong(input.getString(MaraUtils.finalOrderingColumns.indexOf("svc_service_frm_date")))
      val paid_amount = if (StringUtility.isNull(input.getString(MaraUtils.finalOrderingColumns.indexOf("rev_paid_amt")))) 0D
      else input.getString(MaraUtils.finalOrderingColumns.indexOf("rev_paid_amt")).toDouble
      val allowed_amount = if (StringUtility.isNull(input.getString(MaraUtils.finalOrderingColumns.indexOf("rev_allowed_amt")))) 0D
      else input.getString(MaraUtils.finalOrderingColumns.indexOf("rev_allowed_amt")).toDouble

      val memberId = input.getString(MaraUtils.finalOrderingColumns.indexOf("dw_member_id"))
      println(memberId+DateUtility.convertLongToString(MaraUtils.dataPeriodStartDate)+"::::::::MEDICAL "+serviceFromDate+"::::::::: "+DateUtility.convertLongToString(MaraUtils.clientConfigCycleEndDate))
      if (MaraUtils.isClaimWithinTwelveMonths(DateUtility.convertStringToLong(serviceFromDate), MaraUtils.dataPeriodStartDate, MaraUtils.clientConfigCycleEndDate)) {
        inputMedClaimList.add(MaraUtils.getInputMedClaim(input))
        paidAmount.put(input.getString(MaraUtils.finalOrderingColumns.indexOf("ins_emp_group_id")), paid_amount)
        allowedAmount.put(input.getString(MaraUtils.finalOrderingColumns.indexOf("ins_emp_group_id")), allowed_amount)
        buffer.update(4, inputMedClaimList)
        buffer.update(5, paidAmount)
        buffer.update(6, allowedAmount)
      }
    }
  }

  def calculateMaraScores(buffer : Row): mutable.HashMap[String, String] = {
    //    val scoreHashMap : mutable.HashMap[String, String] = mutable.HashMap.empty[String, String]
    val member = new InputMember
    val memberInfo = buffer.getList(0)
    val exposureMonths = calculateExposureMonths(buffer.getMap(2).asInstanceOf[Map[Long, Long]])
    member.setMemberId(memberInfo.get(0))
    member.setDob(new Date(DateUtility.convertStringToLong(memberInfo.get(1))))
    member.setDependentStatus(MaraUtils.getDependentStatus(memberInfo.get(2)))
    member.setGender(MaraUtils.getGender(memberInfo.get(4)))
    member.setExposureMonths(exposureMonths)
    member.setInputMedClaim(MaraUtils.getMaraMedClaimObject(buffer.getList(4).toArray()))
    member.setInputRxClaim(MaraUtils.getMaraRxClaimObject(buffer.getList(3).toArray()))
    val maraOutputScores = calculateRiskScores(member, MaraUtils.modelProcessor)
    val modelDataMap = maraOutputScores.getOutputModelDataMap
    val prospectiveModelScore = modelDataMap.get("CXPROLAG0").getOutputModelScore
    val concurrentModelScore = modelDataMap.get("CXCONLAG0").getOutputModelScore

    val conditionMap = modelDataMap.get("CXPROLAG0").getOutputPercentContribution
    var conditionString = ""
    import scala.collection.JavaConversions._
    for (conditionCode <- conditionMap.keySet()) {
      var conditionDescription = conditionMapFromFile.getOrElse(conditionCode, "Unknown Code")
      conditionString += conditionCode+"^%~"+conditionDescription+"^%~"+conditionMap.get(conditionCode)+"^*~"
    }
    var riskAA = ""
    var totalPaidAmount = 0D
    var totalAllowedAmount = 0D
    val groupwisePaidAmounts = buffer.getMap(5).asInstanceOf[Map[String, Double]]
    val groupwiseAllowedAmounts = buffer.getMap(6).asInstanceOf[Map[String, Double]]
    for (groupWiseEntry <- groupwisePaidAmounts) {
      if (riskAA.isEmpty){
        riskAA = groupWiseEntry._1 + "^%~" + groupWiseEntry._2 + ";" + groupwiseAllowedAmounts(groupWiseEntry._1)
        totalPaidAmount += groupWiseEntry._2
        totalAllowedAmount += groupwiseAllowedAmounts(groupWiseEntry._1)
      }
      else{
        riskAA = riskAA + "^*~" + groupWiseEntry._1 + "^%~" + groupWiseEntry._2 + ";" + groupwiseAllowedAmounts(groupWiseEntry._1)
        totalPaidAmount += groupWiseEntry._2
        totalAllowedAmount += groupwiseAllowedAmounts(groupWiseEntry._1)
      }
    }
    mutable.HashMap(
      "mbr_dob" -> memberInfo.get(1),
      "mbr_relationship_code" -> memberInfo.get(2),
      "mbr_relationship_desc" -> memberInfo.get(3),
      "mbr_gender" -> memberInfo.get(4),
      "unblindMemberId" -> memberInfo.get(5),
      "mbr_current_status" -> memberInfo.get(6),
      "memberFullName" -> memberInfo.get(7),
      "ins_emp_group_id" -> memberInfo.get(8),
      "ins_emp_group_name" -> memberInfo.get(9),
      "ins_division_id" -> memberInfo.get(10),
      "ins_carrier_id" -> memberInfo.get(11),
      "ins_plan_id" -> memberInfo.get(12),
      "udf16" -> memberInfo.get(13),
      "udf17" -> memberInfo.get(14),
      "udf18" -> memberInfo.get(15),
      "udf19" -> memberInfo.get(16),
      "udf20" -> memberInfo.get(17),
      "udf21" -> memberInfo.get(18),
      "udf22" -> memberInfo.get(19),
      "udf23" -> memberInfo.get(20),
      "udf24" -> memberInfo.get(21),
      "udf25" -> memberInfo.get(22),
      "ins_plan_type_code" -> memberInfo.get(23),
      "integer_member_id" -> memberInfo.get(24),
      "exposureMonths" -> exposureMonths.toString,
      "prospectiveInpatientRaw" -> prospectiveModelScore.getHipScore.toString,
      "prospectiveOutpatientRaw"  -> prospectiveModelScore.getHopScore.toString,
      "prospectiveMedicalRaw" -> prospectiveModelScore.getMedScore.toString,
      "prospectivePharmacyRaw" -> prospectiveModelScore.getRxScore.toString,
      "prospectivePhysicianRaw" -> prospectiveModelScore.getPhyScore.toString,
      "prospectiveTotalScoreRaw" -> prospectiveModelScore.getTotScore.toString,
      "prospectiveERScoreRaw" -> prospectiveModelScore.getErScore.toString,
      "prospectiveOtherScoreRaw" -> prospectiveModelScore.getOthScore.toString,
      "concurrentInpatientRaw" -> concurrentModelScore.getHipScore.toString,
      "concurrentOutpatientRaw" -> concurrentModelScore.getHopScore.toString,
      "concurrentMedicalRaw" -> concurrentModelScore.getMedScore.toString,
      "concurrentPharmacyRaw" -> concurrentModelScore.getRxScore.toString,
      "concurrentPhysicianRaw" -> concurrentModelScore.getPhyScore.toString,
      "concurrentTotalScoreRaw" -> concurrentModelScore.getTotScore.toString,
      "concurrentERScoreRaw" -> concurrentModelScore.getErScore.toString,
      "concurrentOtherScoreRaw" -> concurrentModelScore.getOthScore.toString,
      "conditionList" -> conditionString,
      "groupWiseAmounts" -> riskAA,
      "totalPaidAmount" -> totalPaidAmount.toString,
      "totalAllowedAmount" -> totalAllowedAmount.toString)
  }

  private def addEligibleDateRanges(eligibleDateRanges: util.TreeMap[Long, Long], effectiveDate: Long, terminationDate: Long) {
    var effDate = effectiveDate
    var termDate = terminationDate
    if (effDate < termDate) {
      if (termDate == MaraUtils.FUTURE_DATE) termDate = MaraUtils.clientConfigCycleEndDate
      if (termDate > MaraUtils.clientConfigCycleEndDate) termDate = MaraUtils.clientConfigCycleEndDate
      if (effDate < MaraUtils.dataPeriodStartDate) effDate = MaraUtils.dataPeriodStartDate

      if (!eligibleDateRanges.isEmpty && eligibleDateRanges.containsKey(effDate) && (eligibleDateRanges.get(effDate) > termDate))
        eligibleDateRanges.put(effDate, eligibleDateRanges.get(effDate))
      else
        eligibleDateRanges.put(effDate, termDate)
    }
  }

  private def calculateExposureMonths(eligibleDateRanges: collection.Map[Long, Long]) = {
    val eligibleMonths = new util.HashSet[String]
    var exposureMonths = 0
    for(entry <- eligibleDateRanges){
      val fromDate = entry._1
      val toDate = entry._2
      var endDate = new DateTime(toDate)
      val months = DateUtility.getDateDiff(fromDate, toDate, 1)
      println(months+" EEEEEEEEEEEEEEEEEEEE "+DateUtility.convertLongToString(fromDate)+" "+DateUtility.convertLongToString(toDate))
      for(i <- 1 to months) {
        eligibleMonths.add(endDate.getMonthOfYear + "-" + endDate.getYear)
        endDate = endDate.minusMonths(1)
      }
    }
    import scala.collection.JavaConverters._
    for(eligibleMonthYear <- eligibleMonths.asScala) {
      println("ooooooooooo "+eligibleMonthYear)
      if (dataPeriodMonthYears.contains(eligibleMonthYear)){
        println("iiiiiiiiiii "+eligibleMonthYear)
        exposureMonths += 1}
    }
    println(exposureMonths)
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
        throw new RuntimeException
      }
      case ne: NullPointerException => {
        System.out.println("Invalid users : " + inputMember.getMemberId)
        System.out.println(ne.getLocalizedMessage)
        ne.printStackTrace(System.out)
      }
    }
    outputMaraResultSet
  }

  def tryRecalculatingRiskScoreForInvalidMembers(inputMember: InputMember, modelProcessor: ModelProcessor) = {
    val age = Years.yearsBetween(new DateTime(inputMember.getDob.getTime), new DateTime(MaraUtils.clientConfigCycleEndDate)).getYears
    System.out.println("Invalid Age " + age + " CycleEndDate " + new Date(MaraUtils.clientConfigCycleEndDate) + " Member Id " + inputMember.getMemberId)
    val cycleEndDate = new DateTime(MaraUtils.clientConfigCycleEndDate)
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
