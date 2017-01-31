package officework

import org.apache.spark.sql.types._

/**
  * Created by ramaharjan on 1/24/17.
  */
object Validations {

  def convertTypes(value: String, struct: StructField): Any = struct.dataType match {
    case DoubleType => if(!value.isEmpty && formatValidator(value, struct)) value.toDouble else null
    case FloatType => if(!value.isEmpty && formatValidator(value, struct)) value.toFloat else null
    case DateType => if(!value.isEmpty && formatValidator(value, struct)) java.sql.Date.valueOf(value.toString) else null
    case IntegerType => if(!value.isEmpty && formatValidator(value, struct)) value.toInt else null
    case LongType => if(!value.isEmpty && formatValidator(value, struct)) value.toLong else null
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
