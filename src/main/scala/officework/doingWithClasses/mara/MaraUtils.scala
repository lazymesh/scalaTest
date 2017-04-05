package officework.doingWithClasses.mara

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import main.scala.officework.doingWithClasses.{ClientCfgParameters, DateUtility}
import main.scala.officework.doingWithObjects.DateUtils
import milliman.mara.exception.{MARAClassLoaderException, MARALicenseException}
import milliman.mara.model._
import officework.doingWithClasses.StringUtility
import org.apache.spark.SparkFiles
import org.apache.spark.sql.Row
import org.joda.time.DateTime

import scala.io.Source

/**
  * Created by ramaharjan on 3/13/17.
  */
object MaraUtils extends Serializable{

  val clientConfig = new ClientCfgParameters("/client_config.properties")
  val modelProcessor : ModelProcessor = prepareModelProcessor(DateUtility.convertStringToLong(clientConfig.getEOC()))

  val clientConfigCycleEndDate = DateUtility.convertStringToLong(clientConfig.getEOC())
  val dataPeriodStartDate = new DateTime(clientConfigCycleEndDate).minusMonths(12).getMillis

  //  var endOfCycleDate : String = _
  val FUTURE_DATE = DateUtility.convertStringToLong("2099-12-31")

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

  val maraRawOutput = Vector("dw_member_id", "mbr_dob", "mbr_relationship_code", "mbr_relationship_desc", "mbr_gender", "unblindMemberId",
    "mbr_current_status", "memberFullName", "ins_emp_group_id", "ins_emp_group_name", "ins_division_id", "ins_carrier_id",
    "ins_plan_id", "udf16", "udf17", "udf18", "udf19", "udf20", "udf21", "udf22", "udf23", "udf24", "udf25", "ins_plan_type_code",
    "integer_member_id", "exposureMonths", "prospectiveInpatientRaw", "prospectiveOutpatientRaw", "prospectiveMedicalRaw",
    "prospectivePharmacyRaw", "prospectivePhysicianRaw", "prospectiveTotalScoreRaw", "prospectiveERScoreRaw", "prospectiveOtherScoreRaw",
    "concurrentInpatientRaw", "concurrentOutpatientRaw", "concurrentMedicalRaw", "concurrentPharmacyRaw", "concurrentPhysicianRaw",
    "concurrentTotalScoreRaw", "concurrentERScoreRaw", "concurrentOtherScoreRaw", "conditionList", "groupWiseAmounts", "totalPaidAmount",
    "totalAllowedAmount")

  def getModelProcessor() : ModelProcessor = {
    modelProcessor
  }

  def getMemberFullName(input: Row): String = {
    val firstName = input.getString(MaraUtils.finalOrderingColumns.indexOf("mbr_first_name"))
    val middleName = input.getString(MaraUtils.finalOrderingColumns.indexOf("mbr_middle_name"))
    val lastName = input.getString(MaraUtils.finalOrderingColumns.indexOf("mbr_last_name"))
    lastName + ", " + firstName + (if (!middleName.isEmpty) " " + middleName else "")
  }

  def isClaimWithinTwelveMonths(serviceDate: Long, dataPeriodStartDate: Long, dataPeriodEndDate: Long): Boolean = serviceDate > dataPeriodStartDate && serviceDate <= dataPeriodEndDate

  def getInputMedClaim(claimLine: Row): String = {
    var inputMedClaim = ""
    var serviceFromDate = if(StringUtility.isNotNull(claimLine.getString(MaraUtils.finalOrderingColumns.indexOf("svc_service_frm_date"))))
      claimLine.getString(MaraUtils.finalOrderingColumns.indexOf("svc_service_frm_date")) else "2099-12-31"
    val serviceToDate = if(StringUtility.isNotNull(claimLine.getString(MaraUtils.finalOrderingColumns.indexOf("svc_service_to_date"))))
      claimLine.getString(MaraUtils.finalOrderingColumns.indexOf("svc_service_to_date")) else "2099-12-31"
    val revPaidDate = if(StringUtility.isNotNull(claimLine.getString(MaraUtils.finalOrderingColumns.indexOf("rev_paid_date"))))
      claimLine.getString(MaraUtils.finalOrderingColumns.indexOf("rev_paid_date")) else "2099-12-31"
    val billedAmount = if (StringUtility.isNull(claimLine.getString(MaraUtils.finalOrderingColumns.indexOf("rev_billed_amt")))) 0D
    else claimLine.getString(MaraUtils.finalOrderingColumns.indexOf("rev_billed_amt")).toDouble
    val paidAmount = if (StringUtility.isNull(claimLine.getString(MaraUtils.finalOrderingColumns.indexOf("rev_paid_amt")))) 0D
    else claimLine.getString(MaraUtils.finalOrderingColumns.indexOf("rev_paid_amt")).toDouble
    val allowedAmount = if (StringUtility.isNull(claimLine.getString(MaraUtils.finalOrderingColumns.indexOf("rev_allowed_amt")))) 0D
    else claimLine.getString(MaraUtils.finalOrderingColumns.indexOf("rev_allowed_amt")).toDouble
    val cptCode = claimLine.getString(MaraUtils.finalOrderingColumns.indexOf("svc_cpt_code"))
    val hcpcsCode = claimLine.getString(MaraUtils.finalOrderingColumns.indexOf("svc_hcpcs_code"))
    val icdCode = claimLine.getString(MaraUtils.finalOrderingColumns.indexOf("svc_icd_proc_1_code"))
    val procedureCode = if(!cptCode.isEmpty) cptCode else if (!hcpcsCode.isEmpty) hcpcsCode else if(!icdCode.isEmpty) icdCode else ""

    inputMedClaim = inputMedClaim + claimLine.getString(MaraUtils.finalOrderingColumns.indexOf("dw_member_id")) + "::"+
      claimLine.getString(MaraUtils.finalOrderingColumns.indexOf("rev_claim_id")) + "::"+
      claimLine.getString(MaraUtils.finalOrderingColumns.indexOf("rev_claim_line_id")) + "::"+
      serviceFromDate + "::"+
      serviceToDate + "::"+
      revPaidDate + "::"+
      claimLine.getString(MaraUtils.finalOrderingColumns.indexOf("svc_drg_code")) + "::"+
      claimLine.getString(MaraUtils.finalOrderingColumns.indexOf("svc_rev_code")) + "::"+
      claimLine.getString(MaraUtils.finalOrderingColumns.indexOf("prv_service_provider_id")) + "::"+
      claimLine.getString(MaraUtils.finalOrderingColumns.indexOf("svc_pos_code")) + "::"+
      billedAmount + "::"+
      paidAmount + "::"+
      allowedAmount + "::"
    for (i <- 1 to 9) {
      inputMedClaim = inputMedClaim + claimLine.getString(MaraUtils.finalOrderingColumns.indexOf("svc_diag_" + i + "_code")) + "::"
    }

    inputMedClaim = inputMedClaim + claimLine.getString(MaraUtils.finalOrderingColumns.indexOf("svc_pos_code")) + "::"+procedureCode

    inputMedClaim
  }

  def getInputRxClaim(rxLine: Row): String = {
    var inputRxClaim = ""
    val filledDate = if(StringUtility.isNotNull(rxLine.getString(MaraUtils.finalOrderingColumns.indexOf("rx_svc_filled_date"))))
      rxLine.getString(MaraUtils.finalOrderingColumns.indexOf("rx_svc_filled_date")) else "2099-12-31"
    val billedAmount = if (StringUtility.isNull(rxLine.getString(MaraUtils.finalOrderingColumns.indexOf("rev_billed_amt")))) 0D
    else rxLine.getString(MaraUtils.finalOrderingColumns.indexOf("rev_billed_amt")).toDouble
    val paidAmount = if (StringUtility.isNull(rxLine.getString(MaraUtils.finalOrderingColumns.indexOf("rev_paid_amt")))) 0D
    else rxLine.getString(MaraUtils.finalOrderingColumns.indexOf("rev_paid_amt")).toDouble
    val daysOfSupply = if (StringUtility.isNull(rxLine.getString(MaraUtils.finalOrderingColumns.indexOf("svc_days_of_supply")))) 0D
    else rxLine.getString(MaraUtils.finalOrderingColumns.indexOf("svc_days_of_supply")).toDouble
    val unitQty = if (StringUtility.isNull(rxLine.getString(MaraUtils.finalOrderingColumns.indexOf("svc_unit_qty")))) 0D
    else rxLine.getString(MaraUtils.finalOrderingColumns.indexOf("svc_unit_qty")).toDouble
    val allowedAmount = if (StringUtility.isNull(rxLine.getString(MaraUtils.finalOrderingColumns.indexOf("rev_allowed_amt")))) 0D
    else rxLine.getString(MaraUtils.finalOrderingColumns.indexOf("rev_allowed_amt")).toDouble


    inputRxClaim = inputRxClaim + rxLine.getString(MaraUtils.finalOrderingColumns.indexOf("dw_member_id")) + "::" +
      rxLine.getString(MaraUtils.finalOrderingColumns.indexOf("svc_ndc_code")) + "::" +
      rxLine.getString(MaraUtils.finalOrderingColumns.indexOf("rev_transaction_num")) + "::" +
      filledDate + "::" +
      rxLine.getString(MaraUtils.finalOrderingColumns.indexOf("prv_prescriber_id")) + "::" +
      billedAmount + "::" +
      paidAmount + "::" +
      daysOfSupply + "::" +
      unitQty + "::" +
      allowedAmount

    inputRxClaim
  }

  def getMaraMedClaimObject(medicalArrayList : Array[AnyRef]) : util.ArrayList[InputMedClaim] = {
    val inputMedClaimList = new util.ArrayList[InputMedClaim]
    //    println(":::::::::::::::: "+medicalArrayList(0))
    for(claim <- medicalArrayList) {
      val claimLine = claim.toString.split("::")
      val inputMedClaim = new InputMedClaim

      inputMedClaim.setMemberId(claimLine(0))
      inputMedClaim.setClaimId(claimLine(1))
      inputMedClaim.setClaimSeq(claimLine(2))
      inputMedClaim.setFromDate(new Date(DateUtility.convertStringToLong(claimLine(3))))
      inputMedClaim.setToDate(new Date(DateUtility.convertStringToLong(claimLine(4))))
      inputMedClaim.setPaidDate(new Date(DateUtility.convertStringToLong(claimLine(5))))
      inputMedClaim.setDrg(claimLine(6))
      inputMedClaim.setRevCode(claimLine(7))
      inputMedClaim.setDrgVersion("")
      inputMedClaim.setSpecialty("")
      inputMedClaim.setProviderId(claimLine(8))
      inputMedClaim.setPos(claimLine(9))
      val billed_amount = if (!claimLine(10).isEmpty) claimLine(10).toDouble else 0D
      val paid_amount = if (!claimLine(11).isEmpty) claimLine(11).toDouble else 0D
      val allowed_amount = if (!claimLine(12).isEmpty && claimLine(12).toDouble > 0D) claimLine(12).toDouble else paid_amount
      inputMedClaim.setCharged(billed_amount)
      inputMedClaim.setPaid(paid_amount)
      inputMedClaim.setAllowed(allowed_amount)
      val diagList = new util.ArrayList[String]
      for(i <- 13 to 21) {
        diagList.add(claimLine(i))
      }
      inputMedClaim.setDiagList(diagList)

      //inputMedClaim.setProcCode(claimLine(22))
      inputMedClaimList.add(inputMedClaim)
    }
    inputMedClaimList
  }
  def getMaraRxClaimObject(rxArrayList : Array[AnyRef]) : util.ArrayList[InputRxClaim] = {
    val inputRxClaimList = new util.ArrayList[InputRxClaim]
    for(rxClaim <- rxArrayList) {
      val rxLine = rxClaim.toString.split("::")
      val inputRxClaim = new InputRxClaim

      inputRxClaim.setMemberId(rxLine(0))
      inputRxClaim.setNdcCode(rxLine(1))
      inputRxClaim.setClaimId(rxLine(2))
      inputRxClaim.setFillDate(new Date(DateUtility.convertStringToLong(rxLine(3))))
      inputRxClaim.setProviderId(rxLine(4))
      val billed_amount = if (!rxLine(5).isEmpty) rxLine(5).toDouble else 0D
      val paid_amount = if (!rxLine(6).isEmpty) rxLine(6).toDouble else 0D
      val daysOfSupply = if (!rxLine(7).isEmpty) rxLine(7).toDouble else 0D
      val svcQuantity = if (!rxLine(7).isEmpty) rxLine(8).toDouble else 0D
      val allowed_amount = if (!rxLine(8).isEmpty && rxLine(8).toDouble > 0D) rxLine(8).toDouble else paid_amount
      inputRxClaim.setCharged(billed_amount)
      inputRxClaim.setPaid(paid_amount)
      inputRxClaim.setDaysSupplied(daysOfSupply)
      inputRxClaim.setQtyDispensed(svcQuantity)
      inputRxClaim.setAllowed(paid_amount)
      inputRxClaimList.add(inputRxClaim)
    }
    inputRxClaimList
  }

  def getDependentStatus(relationshipCode: String): InputEnums.DependentStatus = {
    if (relationshipCode.equalsIgnoreCase("E")) InputEnums.DependentStatus.POLICYHOLDER
    else if (relationshipCode.equalsIgnoreCase("S")) InputEnums.DependentStatus.SPOUSE
    else if (relationshipCode.equalsIgnoreCase("D")) InputEnums.DependentStatus.CHILD
    else InputEnums.DependentStatus.OTHER
  }

  def getGender(gender: String): InputEnums.Gender = {
    if (gender.equalsIgnoreCase("m")) InputEnums.Gender.MALE
    else InputEnums.Gender.FEMALE
  }

  def prepareModelProcessor(endOfCycleDate: Long): ModelProcessor = {
    System.out.println("License File Location : " + SparkFiles.get("mara.lic"))
    System.out.println("Mara Data 1 Folder Location: " + SparkFiles.get("MARA1.dat"))
    System.out.println("Mara Data 2 Folder Location: " + SparkFiles.get("MARA2.dat"))
    try {
      new ModelProcessor(setupModelProperties(endOfCycleDate))
    }
    catch {
      case e: MARAClassLoaderException => {
        e.printStackTrace()
        System.out.println("MaraClassLoader Exception occurred in setup")
        throw new RuntimeException(e)
      }
      case e: MARALicenseException => {
        System.out.println("MaraLicense exception occurred in setup")
        e.printStackTrace()
        throw new RuntimeException(e)
      }
    }
  }

  def setupModelProperties(endOfCycleDate: Long): ModelProperties = {
    val modelProperties = new ModelProperties
    var maraFolder = SparkFiles.getRootDirectory()+"/"

    val modelList = new java.util.ArrayList[InputEnums.ModelName]
    modelList.add(InputEnums.ModelName.CXPROLAG0)
    modelList.add(InputEnums.ModelName.CXCONLAG0)
    val df_mmddyyyy = new SimpleDateFormat("MM/dd/yyyy")

    modelProperties.setLicenseFileLocation(SparkFiles.get("mara.lic"))
    modelProperties.setMaraDataFolderLocation(maraFolder)
    modelProperties.setOutputPercentContributions(true)
    modelProperties.setModelList(modelList)
    modelProperties.setEndBasePeriodDate(df_mmddyyyy.format(endOfCycleDate))

    modelProperties
  }

  val clinical_classifications_file = "/mara/Milliman_Clinical_Classifications_MARA_2_2_4.csv"
  def getConditionMap() = {
    val rawData = Source.fromInputStream(getClass.getResourceAsStream(clinical_classifications_file)).getLines().filter(!_.startsWith("#"))
    rawData.map(x=>x.split(":", -1)).map(value => value(0) -> value(1)).toMap
  }

  def debugMember(inputMember: InputMember) {
    println("MemberId" + inputMember.getMemberId)
    println("Dob" + inputMember.getDob) //new Date(memberEligibility.getMemberDOB()) );
    println("Dependent " + inputMember.getDependentStatus)
    println("Gender " + inputMember.getGender)
    println("Exposure " + inputMember.getExposureMonths)
    val listMedicalClaims = inputMember.getInputMedClaim
    val listRxClaims = inputMember.getInputRxClaim
    println("Number of Medical Claim " + listMedicalClaims.size)
    import scala.collection.JavaConversions._
    for (claim <- listMedicalClaims) {
      println("memberId :" + claim.getMemberId)
      println("claimId : " + claim.getClaimId)
      println("claimSeq :" + claim.getClaimSeq)
      println("providerId :" + claim.getProviderId)
      println("speciality :" + claim.getSpecialty)
      println("allowed :" + claim.getAllowed)
      println("charged :" + claim.getCharged)
      println("paid :" + claim.getPaid)
      println("drg :" + claim.getDrg)
      println("drgVersion :" + claim.getDrgVersion)
      println("fromDate :" + claim.getFromDate)
      println("toDate :" + claim.getToDate)
      println("paidDate :" + claim.getPaidDate)
      println("pos :" + claim.getPos)
      println("revCode :" + claim.getRevCode)
      println("procCode :" + claim.getProcCode)
      println("Diag Code List")
      for (s <- claim.getDiagList) {
        println(s)
      }
    }
    println("Number of Rx Claim" + listRxClaims.size)
    for (claim <- listRxClaims) {
      println("claimId  :" + claim.getClaimId)
      println("memberId :" + claim.getMemberId)
      println("ndcCode  :" + claim.getNdcCode)
      println("proovideId :" + claim.getProviderId)
      println("allowed :" + claim.getAllowed)
      println("charged :" + claim.getCharged)
      println("daysSuplied :" + claim.getDaysSupplied)
      println("fillDate :" + claim.getFillDate)
      println("paid :" + claim.getPaid)
      println("qtyDispensed :" + claim.getQtyDispensed)
    }
  }
}
