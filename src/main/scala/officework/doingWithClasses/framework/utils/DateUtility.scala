package officework.doingWithClasses.framework.utils

import java.sql.Date
import java.text.SimpleDateFormat
import java.time.{LocalDate, Period}
import java.util.Calendar

import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

/**
  * Created by ramaharjan on 3/20/17.
  */
object DateUtility extends Serializable{
  val GMT_DATE_FORMAT : DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd")
  val stringInputFormat = new SimpleDateFormat("yyyy-MM-dd")

  def convertStringToLong(inputDate : String): Long ={
    var dateStr = inputDate
    //    if (StringUtility.isNull(inputDate)) dateStr = "2099-12-31"
    try
      return GMT_DATE_FORMAT.parseMillis(dateStr)

    catch {
      case ex: Exception => {
        val longDate = stringInputFormat.parse(dateStr)
        val dateString = getDateString(longDate)
        convertStringToLong(dateString)
      }
    }
  }

  /*  def getDateString(longDate: Long): String = {
      val date = new Date(longDate)
      getDateString(date)
    }*/

  def getDateString(date: java.util.Date): String = {
    val df = new SimpleDateFormat("yyyy-MM-dd")
    df.format(date)
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

  def getDateDiff(dateFrom: Long, dateTo: Long, d: Int): Int = {
    val period = Period.between(LocalDate.ofEpochDay(dateFrom), LocalDate.ofEpochDay(dateTo))
    d match {
      case 0 => return period.getDays
      case 1 => return period.getMonths
      case 2 => return period.getYears
    }
  }
}
