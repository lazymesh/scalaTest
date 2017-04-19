package main.scala.officework.doingWithClasses.mara


import officework.doingWithClasses.mara.{MaraUDAF, MaraUtils}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.SparkContext
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lit, row_number, udf}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

import scala.collection.mutable


/**
  * Created by ramaharjan on 3/2/17.
  */
class MaraAssembly(eligDataFrame : DataFrame, medDataFrame : DataFrame, rxDataFrame : DataFrame, sc : SparkContext, sQLContext: SQLContext) {

  var eligDF = eligDataFrame
  var medDF = medDataFrame
  var rxDF = rxDataFrame

  val dfsWorkingDir = FileSystem.get(new Configuration()).getWorkingDirectory
  val mara1DatFile = "/mara3_9_0/MARA1.dat"
  val mara2DatFile = "/mara3_9_0/MARA2.dat"
  val maraLicenseFile = "/mara3_9_0/mara.lic"
  sc.addFile(dfsWorkingDir+mara1DatFile)
  sc.addFile(dfsWorkingDir+mara2DatFile)
  sc.addFile(dfsWorkingDir+maraLicenseFile)


  def setSortDate = udf((date: String) => date )

  def maraCalculator() : DataFrame = {

    //todo groupfilter of elig
    eligDF = eligDF.select(MaraUtils.eligRetain.map(col): _*)
    for (column <- 0 until MaraUtils.insertToEligColumns.size) {
      eligDF = eligDF.withColumn(MaraUtils.insertToEligColumns(column), lit(MaraUtils.insertToEligValues(column)))
    }
    eligDF = eligDF.withColumn(MaraUtils.sortDateColumn, setSortDate(eligDF("ins_med_term_date")))
    eligDF = eligDF.select(MaraUtils.finalOrderingColumns.map(col):_*)

    val window = Window.partitionBy(col("dw_member_id")).orderBy(col("ins_med_term_date").desc, col("ins_med_eff_date").desc, col("mbr_relationship_code").desc, col("ins_emp_group_id").desc)
    val latestEligDF = eligDF.withColumn("rn", row_number.over(window)).where(col("rn") === 1).drop(col("rn")).withColumn(MaraUtils.inputTypeFlagColumn(0), lit(MaraUtils.inputTypeFlagEligLatest(0)))

    //todo groupfilter of medical
    medDF = medDF.select(MaraUtils.medRetain.map(col): _*)
    for (column <- 0 until MaraUtils.insertToMedColumns.size) {
      medDF = medDF.withColumn(MaraUtils.insertToMedColumns(column), lit(MaraUtils.insertToMedValues(column)))
    }
    medDF = medDF.withColumn(MaraUtils.sortDateColumn, setSortDate(medDF("svc_service_frm_date")))
    //todo convert "svc_service_frm_date", "svc_service_to_date", "rev_paid_date", "ins_med_eff_date", "ins_med_term_date" to date
    medDF = medDF.select(MaraUtils.finalOrderingColumns.map(col):_*)

    //todo groupfilter of pharmacy
    rxDF = rxDF.withColumnRenamed("svc_service_frm_date", "rx_svc_filled_date")
    rxDF = rxDF.select(MaraUtils.rxRetain.map(col): _*)
    for (column <- 0 until MaraUtils.insertToRxColumns.size) {
      rxDF = rxDF.withColumn(MaraUtils.insertToRxColumns(column), lit(MaraUtils.insertToRxValues(column)))
    }
    rxDF = rxDF.withColumn(MaraUtils.sortDateColumn, setSortDate(rxDF("rx_svc_filled_date")))
    //todo convert "rx_svc_filled_date","ins_med_eff_date", "ins_med_term_date" to date
    rxDF = rxDF.select(MaraUtils.finalOrderingColumns.map(col):_*)

    var combined = latestEligDF.union(eligDF).union(medDF).union(rxDF)
    val maraUdaf = new MaraUDAF(combined.schema)
    combined = combined.orderBy("inputTypeFlag").groupBy("dw_member_id").agg(
      maraUdaf(MaraUtils.finalOrderingColumns.map(col):_*).as("maraOutput"))

    val maraSchema = StructType(MaraUtils.maraRawOutput.map(field => StructField(field, DataTypes.StringType, true)))

    var maraRddRow = combined.rdd.map(row => {
      val list = mutable.MutableList[String]()
      val memberId = row(0).toString
      val maraOutMap = row(1).asInstanceOf[Map[String, String]]
      for(field <- MaraUtils.maraRawOutput){
        if(field.equalsIgnoreCase("dw_member_id")){
          list += memberId
        }
        else{
          list += maraOutMap.getOrElse(field, field)
        }
      }
      Row.fromSeq(list)
    })
    maraRddRow = maraRddRow.filter(!_.get(1).toString().equalsIgnoreCase("mbr_dob"))
    val maraRawDF = sQLContext.createDataFrame(maraRddRow, maraSchema)

    maraRawDF
  }
}