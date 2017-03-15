package officework.doingWithClasses

import scala.collection.mutable

/**
  * Created by ramaharjan on 3/14/17.
  */
object SummableMap extends mutable.HashMap {
/*  //    @Override
  def put(key: AnyVal, value: AnyVal, separator: String): V = {
    if (!this.containsKey(key)) {
      return super.put(key, value)
    }
    else {
      val `val`: V = this.get(key)
      if (`val`.isInstanceOf[Float]) {
        val v: Float = (`val`.asInstanceOf[Float]).floatValue + (value.asInstanceOf[Float]).floatValue
        return super.put(key, v.asInstanceOf[V])
      }
      else if (`val`.isInstanceOf[String]) {
        val v: String = (`val`.asInstanceOf[String]) + separator + (value.asInstanceOf[String])
        return super.put(key, v.asInstanceOf[V])
      }
      else if (`val`.isInstanceOf[Integer]) {
        val v: Integer = (`val`.asInstanceOf[Integer]) + (value.asInstanceOf[Integer])
        return super.put(key, v.asInstanceOf[V])
      }
      else if (`val`.isInstanceOf[Long]) {
        val v: Long = `val`.asInstanceOf[Long] + value.asInstanceOf[Long]
        return super.put(key, v.asInstanceOf[V])
      }
      else if (`val`.isInstanceOf[Double]) {
        val v: Double = `val`.asInstanceOf[Double] + value.asInstanceOf[Double]
        return super.put(key, v.asInstanceOf[V])
      }
      else throw new RuntimeException("The value type " + value.getClass.getName + " is not supported in this map")
    }
  }

  override def put(key: K, value: V): V = {
    return put(key, value, ";")
  }

  def update(key: K, value: V): V = {
    return put(key, value)
  }*/
}
