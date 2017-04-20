package officework.doingWithClasses.framework.goldenrules

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
  * Created by ramaharjan on 2/1/17.
  */
class GoldenRules(eoc : String, clientType : String) extends GoldenRulesUDFs(eoc, clientType){

  private val U : String = "U"
  private val futureDate : String = "2099-12-31"
  private val bk : String = "BK"
  private val blank : String = "BLANK"
  private val ungroupable : String = "Ungroupable"
  private val other : String = "Other"
  private val unknown : String = "Unknown"
  private val posCode : String = "99"
  private val ub04 : String = "UB04"

  def applyCommonGoldenRules(commonlyChangedTable : DataFrame): DataFrame ={
    //todo find efficient way for applying the rules
    val dobChanged = commonlyChangedTable.withColumn("mbr_dob", goldenRuleDOB(commonlyChangedTable("mbr_dob"), commonlyChangedTable("mbr_relationship_class")))
    val relationshipCodeChanged = dobChanged.withColumn("mbr_relationship_code", goldenRuleRelationshipCode(dobChanged("mbr_relationship_code"), dobChanged("mbr_dob")))
    relationshipCodeChanged.withColumn("mbr_relationship_desc", goldenRuleRelationshipDesc(relationshipCodeChanged("mbr_relationship_code")))
      .withColumn("mbr_gender", goldenRuleForString(relationshipCodeChanged("mbr_gender"), lit(U)))
  }

  def applyEligibilityGoldenRules(eligibilityTable : DataFrame): DataFrame = {
    //todo find efficient way for applying the rules
    val changedEligibilityTable = applyCommonGoldenRules(eligibilityTable)
    changedEligibilityTable.withColumn("mbr_relationship_class", goldenRuleRelationshipDesc(changedEligibilityTable("mbr_relationship_code")))
      .withColumn("ins_med_eff_date", goldenRuleForString(changedEligibilityTable("ins_med_eff_date"), lit(futureDate)))
      .withColumn("ins_med_term_date", goldenRuleForString(changedEligibilityTable("ins_med_term_date"), lit(futureDate)))
  }

  def applyMedialGoldenRules(medicalTable : DataFrame): DataFrame = {
    //todo find efficient way for applying the rules
    val changedMedicalTable = applyCommonGoldenRules(medicalTable)
    changedMedicalTable.withColumn("rev_paid_date", goldenRuleForString(changedMedicalTable("rev_paid_date"), lit(futureDate)))
      .withColumn("svc_service_frm_date", goldenRuleForString(changedMedicalTable("svc_service_frm_date"), lit(futureDate)))
      .withColumn("svc_diag_1_code", goldenRuleForString(changedMedicalTable("svc_diag_1_code"), lit(bk)))
      .withColumn("svc_procedure_code", goldenRuleForString(changedMedicalTable("svc_procedure_code"), lit(bk)))
      .withColumn("prv_service_provider_id", goldenRuleForString(changedMedicalTable("prv_service_provider_id"), lit(bk)))
      .withColumn("svc_procedure_type", goldenRuleForString(changedMedicalTable("svc_procedure_type"), lit(bk)))
      .withColumn("svc_diag_1_desc", goldenRuleForString(changedMedicalTable("svc_diag_1_desc"), lit(blank)))
      .withColumn("svc_procedure_desc", goldenRuleForString(changedMedicalTable("svc_procedure_desc"), lit(blank)))
      .withColumn("prv_first_name", goldenRuleForString(changedMedicalTable("prv_first_name"), lit(blank)))
      .withColumn("svc_procedure_grouper", goldenRuleForString(changedMedicalTable("svc_procedure_grouper"), lit(ungroupable)))
      .withColumn("Proc1_grouper_desc", goldenRuleForString(changedMedicalTable("Proc1_grouper_desc"), lit(ungroupable)))
      .withColumn("Proc1_Subgrouper_id", goldenRuleForString(changedMedicalTable("Proc1_Subgrouper_id"), lit(ungroupable)))
      .withColumn("svc_procedure_sub_grouper", goldenRuleForString(changedMedicalTable("svc_procedure_sub_grouper"), lit(ungroupable)))
      .withColumn("diag1_grouper_id", goldenRuleForString(changedMedicalTable("diag1_grouper_id"), lit(ungroupable)))
      .withColumn("diag1_grouper_desc", goldenRuleForString(changedMedicalTable("diag1_grouper_desc"), lit(ungroupable)))
      .withColumn("diag1_supergrouper_id", goldenRuleForString(changedMedicalTable("diag1_supergrouper_id"), lit(ungroupable)))
      .withColumn("diag1_supergrouper_desc", goldenRuleForString(changedMedicalTable("diag1_supergrouper_desc"), lit(ungroupable)))
      .withColumn("rev_paid_amt", goldenRuleForFloat(changedMedicalTable("rev_paid_amt"), lit(0.toFloat)))
      .withColumn("prv_in_network_flag", goldenRuleForString(changedMedicalTable("prv_in_network_flag"), lit(U)))
      .withColumn("svc_pos_code", goldenRuleForString(changedMedicalTable("svc_pos_code"), lit(posCode)))
      .withColumn("svc_pos_desc", goldenRuleForString(changedMedicalTable("svc_pos_desc"), lit(other)))
      .withColumn("svc_benefit_code", goldenRuleForString(changedMedicalTable("svc_benefit_code"), lit(unknown)))
      .withColumn("svc_benefit_desc", goldenRuleForString(changedMedicalTable("svc_benefit_desc"), lit(unknown)))
      .withColumn("rev_claim_type", goldenRuleForString(changedMedicalTable("rev_claim_type"), lit(ub04)))
      .withColumn("svc_service_qty", goldenRuleForInteger(changedMedicalTable("svc_service_qty"), lit(0)))
      .withColumn("svc_ip_days", goldenRuleForInteger(changedMedicalTable("svc_ip_days"), lit(0)))
  }

  def applyPharmacyGoldenRules(pharmacyTable : DataFrame): DataFrame = {
    //todo find efficient way for applying the rules
    val changedPharmacyTable = applyCommonGoldenRules(pharmacyTable)
    changedPharmacyTable.withColumn("rev_paid_date", goldenRuleForString(changedPharmacyTable("rev_paid_date"), lit(futureDate)))
      .withColumn("svc_service_frm_date", goldenRuleForString(changedPharmacyTable("svc_service_frm_date"), lit(futureDate)))
      .withColumn("rev_paid_amt", goldenRuleForFloat(changedPharmacyTable("rev_paid_amt"), lit(0.toFloat)))
      .withColumn("svc_days_of_supply", goldenRuleForFloat(changedPharmacyTable("svc_days_of_supply"), lit(0.toFloat)))
      .withColumn("rev_copay_amt", goldenRuleForFloat(changedPharmacyTable("rev_copay_amt"), lit(0.toFloat)))
      .withColumn("svc_ndc_code", goldenRuleForString(changedPharmacyTable("svc_ndc_code"), lit(bk)))
      .withColumn("svc_ndc_desc", goldenRuleForString(changedPharmacyTable("svc_ndc_desc"), lit(blank)))
      .withColumn("prv_prescriber_id", goldenRuleForString(changedPharmacyTable("prv_prescriber_id"), lit(bk)))
      .withColumn("prv_prescriber_first_name", goldenRuleForString(changedPharmacyTable("prv_prescriber_first_name"), lit(blank)))
      .withColumn("svc_drug_name", goldenRuleForString(changedPharmacyTable("svc_drug_name"), lit(blank)))
      .withColumn("svc_rx_class_desc", goldenRuleForString(changedPharmacyTable("svc_rx_class_desc"), lit(ungroupable)))
      .withColumn("svc_rx_class_code", goldenRuleForString(changedPharmacyTable("svc_rx_class_code"), lit(U)))
  }

  def applyHRAGoldenRules(hraTable : DataFrame): DataFrame = {
    hraTable.withColumn("mbr_gender", goldenRuleForString(hraTable("mbr_gender"), lit(U)))
      .withColumn("hra_taken_date", goldenRuleForString(hraTable("hra_taken_date"), lit(futureDate)))
  }

  def applyBiometricGoldenRules(biometricTable : DataFrame): DataFrame = {
    biometricTable.withColumn("mbr_gender", goldenRuleForString(biometricTable("mbr_gender"), lit(U)))
      .withColumn("biometric_test_date", goldenRuleForString(biometricTable("biometric_test_date"), lit(futureDate)))
  }

  def applyOutcomeGoldenRules(outcomeTable : DataFrame): DataFrame = {
    outcomeTable.withColumn("goal_end_date", goldenRuleForString(outcomeTable("goal_end_date"), lit(futureDate)))
  }
}
