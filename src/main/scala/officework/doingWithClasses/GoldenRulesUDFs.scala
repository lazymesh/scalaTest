package main.scala.officework.doingWithClasses

import org.apache.spark.sql.functions.udf
import scala.util.Try

/**
  * Created by ramaharjan on 1/30/17.
  */
class GoldenRulesUDFs(eoc : String, clientType : String) extends scala.Serializable{
  val staticValues = new StaticValues
  staticValues.setEOC(eoc)
  staticValues.setClienType(clientType)

  def goldenRuleDOB = udf((dob: String, relationshipClass: String) =>
    if((dob == null) && (relationshipClass.equalsIgnoreCase("dependent") || relationshipClass.equalsIgnoreCase("other"))){
      DateUtility.convertLongToString(DateUtility.subtractYearsFromStringDate(staticValues.getEOC(), 8))
    }
    else if((dob == null) && (relationshipClass.equalsIgnoreCase("employee") || relationshipClass.equalsIgnoreCase("spouse"))){
      DateUtility.convertLongToString(DateUtility.subtractYearsFromStringDate(staticValues.getEOC(), 27))
    }
    else dob
  )

  def goldenRuleRelationshipCode = udf((code: String, dob: String) =>
    if(code.isEmpty) {
      if (DateUtility.getAge(staticValues.getEOC(), dob) > 26 || staticValues.getClientType().equalsIgnoreCase("medicaid") || staticValues.getClientType().equalsIgnoreCase("medicare")) {
        "E"
      }
      else
        "D"
    }
    else code
  )
  def goldenRuleRelationshipDesc = udf((code: String) =>
      if (code.equalsIgnoreCase("d")){ "Dependent"}
      else if (code.equalsIgnoreCase("e") || code.equalsIgnoreCase("s")){ "Employee"}
    else code
  )

  def goldenRuleForDate = udf((value : String, toValue : String) => if(value == null) toValue else value)
  def goldenRuleForString = udf((value : String, toValue : String) => if(value.isEmpty) toValue else value)
  def goldenRuleForDouble = udf((value : Double, toValue : Double) => Try(value) getOrElse(toValue))
  def goldenRuleForFloat = udf((value : Float, toValue : Float) => Try(value) getOrElse(toValue))
  def goldenRuleForInteger = udf((value : Int, toValue : Int) => Try(value) getOrElse(toValue))

}
