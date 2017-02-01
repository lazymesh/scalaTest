package main.scala.officework.doingWithClasses

import org.apache.spark.sql.DataFrame

/**
  * Created by ramaharjan on 2/1/17.
  */
class EligibilityGoldenRules(eoc : String, clientType : String) extends GoldenRulesUDFs(eoc, clientType){

  def applyEligibilityGoldenRules(eligibilityTable : DataFrame): DataFrame = {
    //todo find efficient way for applying the rules
    val dobChanged = eligibilityTable.withColumn("mbr_dob", eligGoldenRuleDOB(eligibilityTable("mbr_dob"), eligibilityTable("mbr_relationship_class")))
    val relationshipCodeChanged = dobChanged.withColumn("mbr_relationship_code", eligGoldenRuleRelationshipCode(dobChanged("mbr_relationship_code"), dobChanged("mbr_dob")))
    relationshipCodeChanged.withColumn("mbr_relationship_desc", eligGoldenRuleRelationshipDesc(relationshipCodeChanged("mbr_relationship_code")))
      .withColumn("mbr_relationship_class", eligGoldenRuleRelationshipDesc(relationshipCodeChanged("mbr_relationship_code")))
      .withColumn("mbr_gender", eligGoldenRuleGender(relationshipCodeChanged("mbr_gender")))
      .withColumn("ins_med_eff_date", eligGoldenRuleDates(relationshipCodeChanged("ins_med_eff_date")))
      .withColumn("ins_med_term_date", eligGoldenRuleDates(relationshipCodeChanged("ins_med_term_date")))
  }
}
