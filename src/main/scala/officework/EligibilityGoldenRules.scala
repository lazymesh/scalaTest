package main.scala.officework

import officework.DateUtils
import org.apache.spark.sql.functions.udf

/**
  * Created by ramaharjan on 1/30/17.
  */
class EligibilityGoldenRules(eoc : String, clientType : String) extends scala.Serializable{
  var staticValues = new StaticValues
  staticValues.setEOC(eoc)
  staticValues.setClienType(clientType)

  def eligGoldenRuleDOB = udf((dob: String, relationshipClass: String) =>
    if((dob == null) && (relationshipClass.equalsIgnoreCase("dependent") || relationshipClass.equalsIgnoreCase("other"))){
      DateUtils.convertLongToString(DateUtils.subtractYearsFromStringDate(staticValues.getEOC(), 8))
    }
    else if((dob == null) && (relationshipClass.equalsIgnoreCase("employee") || relationshipClass.equalsIgnoreCase("spouse"))){
      DateUtils.convertLongToString(DateUtils.subtractYearsFromStringDate(staticValues.getEOC(), 27))
    }
    else dob
  )

  def eligGoldenRuleRelationshipCode = udf((code: String, dob: String) =>
    if(code.isEmpty) {
      if (DateUtils.getAge(staticValues.getEOC(), dob) > 26 || staticValues.getClientType().equalsIgnoreCase("medicaid") || staticValues.getClientType().equalsIgnoreCase("medicare")) {
        "E"
      }
      else
        "D"
    }
    else code
  )
  def eligGoldenRuleRelationshipDesc = udf((code: String) =>
    if (code.equalsIgnoreCase("d")){ "Dependent"}
    else if (code.equalsIgnoreCase("e") || code.equalsIgnoreCase("s")){ "Employee"}
    else code
  )

  def eligGoldenRuleGender = udf((gender: String) => if(gender.isEmpty) "U" else gender)
  def eligGoldenRuleDates = udf((date: String) => if(date == null) "2099-12-31" else date)

}
