package officework.doingWithClasses

import scala.collection.mutable

/**
  * Created by ramaharjan on 3/16/17.
  */
class SummableMap[K, V] extends mutable.HashMap[K, V] {

  def put (key: K, value: V, separator: String): Option[V] = {
    if (!this.contains(key)) {
      super.put(key, value)
    }
    else {
      val p_value = this.get(key).get
      value match{
        case value : Double => {
          val v: Double = p_value.asInstanceOf[Double] + value
          super.put(key, v.asInstanceOf[V] )
        }
        case value : Float => {
          val v: Float = p_value.asInstanceOf[Float] + value
          super.put(key, v.asInstanceOf[V] )
        }
        case value : Int => {
          val v: Int = p_value.asInstanceOf[Int] + value
          super.put(key, v.asInstanceOf[V] )
        }
        case value : Long => {
          val v: Long = p_value.asInstanceOf[Long] + value
          super.put(key, v.asInstanceOf[V] )
        }
        case value : String => {
          val v: String = p_value.asInstanceOf[String] +separator+ value
          super.put(key, v.asInstanceOf[V] )
        }
        case _ => throw new RuntimeException ("The value type " + value.getClass.getName + " is not supported in this map")
      }
    }
  }

  override def put (key: K, value: V): Option[V] = {
    put(key, value, ";")
  }
}
