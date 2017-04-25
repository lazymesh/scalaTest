package officework.doingWithClasses.framework.utils

import org.apache.spark.sql.types._

import scala.util.Try

/**
  * Created by ramaharjan on 4/21/17.
  */
object Validator {
  def apply(recordType: String, value: String, struct: StructField): Any = struct.dataType match {
    case DoubleType => {
      if (!value.isEmpty && !value.toString.matches(Patterns.FLOAT_PATTERN)) {
        throwRuntimeException(struct.name, struct.dataType, value, recordType)
      }
      else {
        Try(value.toDouble) getOrElse 0D
      }
    }
    case FloatType => {
      if (!value.isEmpty && !value.toString.matches(Patterns.FLOAT_PATTERN)) {
        throwRuntimeException(struct.name, struct.dataType, value, recordType)
      }
      else {
        Try(value.toFloat) getOrElse 0F
      }
    }
    case DateType => {
      if (!value.isEmpty && !value.toString.matches(Patterns.DATE_PATTERN)) {
        throwRuntimeException(struct.name, struct.dataType, value, recordType)
      }
      else {
        Try(java.sql.Date.valueOf(value.toString)) getOrElse java.sql.Date.valueOf("2099-12-31")
      }
    }
    case IntegerType => {
      if (!value.isEmpty && !value.toString.matches(Patterns.INT_PATTERN)) {
        throwRuntimeException(struct.name, struct.dataType, value, recordType)
      }
      else {
        Try(value.toInt) getOrElse 0
      }
    }
    case LongType => {
      if (!value.isEmpty && !value.toString.matches(Patterns.INT_PATTERN)) {
        throwRuntimeException(struct.name, struct.dataType, value, recordType)
      }
      else {
        Try(value.toLong) getOrElse 0L
      }
    }
    case _ => Option(value) getOrElse ""
  }

  def throwRuntimeException(field: String, fieldType: DataType, value: String, recordType: String) : Unit ={
    throw new RuntimeException(recordType + " : Invalid format for field: " + field + " Type: " + fieldType + " Value: " + value)
  }
}
