package officework.doingWithClasses

import java.text.{ParseException, SimpleDateFormat}

/**
  * Created by ramaharjan on 3/15/17.
  */
object StringUtility {

  def removeQuotes (tokens: Array[String] ): Array[String] = {
    val tokenWithoutQuote: Array[String] = new Array[String] (tokens.length)
    var i: Int = 0
    for (item <- tokens) {
      tokenWithoutQuote (i) = item.replaceAll ("\"", "").trim
      i += 1
    }
    return tokenWithoutQuote
  }

  def isNotNull (field: String): Boolean = {
    return ! isNull (field)
  }

  def isNull (field: String): Boolean = {
    if (field == null) return true
    if (field.equalsIgnoreCase ("NULL") || field.trim.equalsIgnoreCase ("") || field.trim.isEmpty) return true
    return false
  }

  def parseInt(value: String, defaultValue: String): Int = {
    var tempValue = value
    if (isNull(value)) tempValue = defaultValue
    tempValue.toInt
  }

  def isCommentEmpty(line: String): Boolean = "" == line.trim || line.startsWith("#")

  def concatFields(field1: String, field2: String, defaultValue: String): String = {
    var concat = ""
    if (!isNull(field1)) concat = field1
    if (!isNull(field2)) concat += " " + field2
    if (isNull(concat.trim)) concat = defaultValue
    concat
  }

  /**
    * @deprecated use com.deerwalk.utils.DateUtils instead
    * @param date
    * @param format
    * @return
    */
  @deprecated def parseDate(date: String, format: String): Long = {
    val simpleDateFormat = new SimpleDateFormat(format)
    try
      simpleDateFormat.parse(date).getTime

    catch {
      case ex: ParseException => {
        throw new RuntimeException(ex)
      }
    }
  }

  def toUpperFirstChar(text: String): String = {
    val c = text.toCharArray
    c(0) = Character.toUpperCase(c(0))
    new String(c)
  }

  def toLowerFirstChar(text: String): String = {
    val c = text.toCharArray
    c(0) = Character.toLowerCase(c(0))
    new String(c)
  }

  /**
    * First checks if given string is empty, if true returns a string with space as character else returns the same string
    *
    * @param value
    * @return
    */
  def toNonEmptyString(value: String): String = {
    if (isNull(value)) return " "
    value
  }

  def concatenateStringArrays(stringArrays: Array[String]*): Array[String] = {
    var len = 0
    for (array <- stringArrays) {
      len += array.length
    }
    val result = new Array[String](len)
    var currentPos = 0
    for (array <- stringArrays) {
      System.arraycopy(array, 0, result, currentPos, array.length)
      currentPos += array.length
    }
    result
  }
}
