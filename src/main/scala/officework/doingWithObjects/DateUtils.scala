package main.scala.officework.doingWithObjects

import java.sql.Date
import java.text.SimpleDateFormat
import java.util.Calendar

/**
  * Created by anahcolus on 1/22/17.
  */
object DateUtils {
  val stringInputFormat = new SimpleDateFormat("yyyy-MM-dd")

  def convertStringToLong(inputDate : String): Long ={
    stringInputFormat.parse(inputDate).getTime
  }

  def convertLongToString(inputDate : Long): String ={
    stringInputFormat.format(inputDate)
  }

  def subtractYearsFromStringDate(date : String, years : Int): Long ={
    val dateAux = Calendar.getInstance()
    dateAux.setTime(new Date(convertStringToLong(date)))
    dateAux.add(Calendar.YEAR, -years)
    dateAux.getTimeInMillis
  }

  def getAge(referenceDate : String, dob : String): Int ={
    val reference = Calendar.getInstance()
      reference.setTime(new Date(convertStringToLong(referenceDate)))
    val dobDate = Calendar.getInstance()
      dobDate.setTime(new Date(convertStringToLong(dob)))
    if (dobDate.getTimeInMillis > reference.getTimeInMillis) {
      throw new IllegalArgumentException("Can't be born in the future");
    }
    var age = reference.get(Calendar.YEAR) - dobDate.get(Calendar.YEAR)
    if(reference.get(Calendar.MONTH) < dobDate.get(Calendar.MONTH)) age -= 1
    if ((reference.get(Calendar.MONTH) == dobDate.get(Calendar.MONTH)) &&
      (reference.get(Calendar.DAY_OF_MONTH) < dobDate.get(Calendar.DAY_OF_MONTH))) age -= 1
    age
  }
}
