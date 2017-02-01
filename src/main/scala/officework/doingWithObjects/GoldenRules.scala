package main.scala.officework.doingWithObjects

import org.apache.spark.sql.functions.udf

/**
  * Created by anahcolus on 1/22/17.
  */
object GoldenRules {

  var eoc : String = ""
  var clientType : String = ""

  def eligGoldenRuleDOB = udf((dob: String, relationshipClass: String) =>
    if(relationshipClass.equalsIgnoreCase("dependent") || relationshipClass.equalsIgnoreCase("other")){
      DateUtils.convertLongToString(DateUtils.subtractYearsFromStringDate(eoc, 8))
    }
    else if(relationshipClass.equalsIgnoreCase("employee") || relationshipClass.equalsIgnoreCase("spouse")){
      DateUtils.convertLongToString(DateUtils.subtractYearsFromStringDate(eoc, 27))
    }
    else dob
  )

  def eligGoldenRuleRelationshipCode = udf((relationshipCode: String, dob: String) =>
    if(DateUtils.getAge(eoc, dob) > 26 || clientType.equalsIgnoreCase("medicaid") || clientType.equalsIgnoreCase("medicare")){
      "E"
    }
    else
      "D"
  )
  def eligGoldenRuleRelationshipDesc = udf((code: String) =>
      if(code.equalsIgnoreCase("d")) "Dependent"
      else if(code.equalsIgnoreCase("e") || code.equalsIgnoreCase("s")) "Employee"
      else code
  )

  def eligGoldenRuleGender = udf((gender: String) => if(gender == null) "U" else gender)
  def eligGoldenRuleDates = udf((date: String) => if(date == null) "2099-12-31" else date)
}
