package officework.doingWithClasses.framework.validation

import officework.Patterns
import org.apache.spark.sql.types._

/**
  * Created by ramaharjan on 2/1/17.
  */
class Validators extends scala.Serializable{

  def convertTypes(value: String, struct: StructField): Any = struct.dataType match {
    case DoubleType => if(!value.isEmpty && formatValidator(value, struct)) value.toDouble else 0D
    case FloatType => if(!value.isEmpty && formatValidator(value, struct)) value.toFloat else 0F
    case DateType => if(!value.isEmpty && formatValidator(value, struct)) java.sql.Date.valueOf(value.toString) else java.sql.Date.valueOf("2099-12-31")
    case IntegerType => if(!value.isEmpty && formatValidator(value, struct)) value.toInt else 0
    case LongType => if(!value.isEmpty && formatValidator(value, struct)) value.toLong else 0L
    case _ => if(!value.isEmpty && formatValidator(value, struct)) value else ""
  }

  def formatValidator(value : String, struct: StructField) : Boolean = {
    if (value == null || StringType.equals(struct.dataType)) {
      //do nothing
    } else if (IntegerType.equals(struct.dataType)) {
      if (!value.toString.matches(Patterns.INT_PATTERN)) {
        throw new RuntimeException("Invalid format for field: " + struct.name + " Type: " + struct.dataType + " Value: " + value)
      }
    } else if (DateType.equals(struct.dataType)) {
      if (!value.toString.matches(Patterns.DATE_PATTERN)) {
        throw new RuntimeException("Invalid format for field: " + struct.name + " Type: " + struct.dataType + " Value: " + value)
      }
    } else if (DoubleType.equals(struct.dataType)) {
      if (!value.toString.matches(Patterns.FLOAT_PATTERN)) {
        throw new RuntimeException("Invalid format for field: " + struct.name + " Type: " + struct.dataType + " Value: " + value)
      }
    } else if (LongType.equals(struct.dataType)) {
      if (!value.toString.matches(Patterns.INT_PATTERN)) {
        throw new RuntimeException("Invalid format for field: " + struct.name + " Type: " + struct.dataType + " Value: " + value)
      }
    } else if (FloatType.equals(struct.dataType)) {
      if (!value.toString.matches(Patterns.FLOAT_PATTERN)) {
        throw new RuntimeException("Invalid format for field: " + struct.name + " Type: " + struct.dataType + " Value: " + value)
      }
    }
    true
  }
}
