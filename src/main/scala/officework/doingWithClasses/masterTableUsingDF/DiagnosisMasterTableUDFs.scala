package main.scala.officework.doingWithClasses.masterTableUsingDF

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.functions.udf

/**
  * Created by ramaharjan on 2/6/17.
  */
class DiagnosisMasterTableUDFs(bc : Broadcast[MasterTableGroupers]) extends scala.Serializable{

  def grouperId = udf((diagCode : String) =>
    getGrouperId(diagCode)
  )

  def grouperIdDesc = udf((diagCode : String) =>
    bc.value.getGrouperIdToDiagGrouperDesc.getOrElse(getGrouperId(diagCode), "Ungroupable")
  )

  def superGrouperId = udf((diagCode : String) =>
    getSuperGrouperId(diagCode)
  )

  def superGrouperIdDesc = udf((diagCode : String) =>
    bc.value.getSuperGrouperIdToSuperGrouperDesc.getOrElse(getSuperGrouperId(diagCode), "Ungroupable")
  )


  private def getGrouperId(diagCode : String): String = {
    bc.value.getCodeToDiagGrouperId.getOrElse(diagCode, "Ungroupable")
  }

  private def getSuperGrouperId(diagCode : String): String = {
    bc.value.getCodeToDiagSuperGrouperId.getOrElse(diagCode, "Ungroupable")
  }

}
