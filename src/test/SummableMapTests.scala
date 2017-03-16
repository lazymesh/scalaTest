import officework.doingWithClasses.SummableMap
import org.scalatest.{BeforeAndAfterEach, FunSuite}

/**
  * Created by ramaharjan on 3/16/17.
  */
class SummableMapTests extends FunSuite with BeforeAndAfterEach {

  test("testing simple summable map for Double"){
    val a = 2D
    val b = 3.2D

    val testMap : SummableMap[String, Double] = new SummableMap[String, Double]
    testMap.put("sum", 8D)
    println("initially : "+testMap.get("sum"))
    testMap.put("sum", a)
    println("adding a : "+testMap.get("sum"))
    testMap.put("sum", b)
    println("adding b : "+testMap.get("sum"))
  }

  test("testing simple summable map for Float"){
    val a = 2F
    val b = 5.2F

    val testMap : SummableMap[String, Float] = new SummableMap[String, Float]
    testMap.put("sum", 8F)
    println("initially : "+testMap.get("sum"))
    testMap.put("sum", a)
    println("adding a : "+testMap.get("sum"))
    testMap.put("sum", b)
    println("adding b : "+testMap.get("sum"))
  }

  test("testing simple summable map for Integer"){
    val a = 3
    val b = 5

    val testMap : SummableMap[String, Int] = new SummableMap[String, Int]
    testMap.put("sum", 8)
    println("initially : "+testMap.get("sum"))
    testMap.put("sum", a)
    println("adding a : "+testMap.get("sum"))
    testMap.put("sum", b)
    println("adding b : "+testMap.get("sum"))
  }

  test("testing simple summable map for Long"){
    val a = 3L
    val b = 4

    val testMap : SummableMap[String, Long] = new SummableMap[String, Long]
    testMap.put("sum", 3L)
    println("initially : "+testMap.get("sum"))
    testMap.put("sum", a)
    println("adding a : "+testMap.get("sum"))
    testMap.put("sum", b)
    println("adding b : "+testMap.get("sum"))
  }

  test("testing simple summable map for String"){
    val a = "abcd"
    val b = "efgh"

    val testMap : SummableMap[String, String] = new SummableMap[String, String]
    testMap.put("sum", "zzzzz")
    println("initially : "+testMap.get("sum"))
    testMap.put("sum", a)
    println("adding a : "+testMap.get("sum"))
    testMap.put("sum", b)
    println("adding b : "+testMap.get("sum"))
  }

  test("testing simple summable map for not supported"){
    val a = Seq("abcd")

    val testMap : SummableMap[String, Any] = new SummableMap[String, Any]
    testMap.put("sum", Seq("zzzzz", "dfsdf"))
    println("initially : "+testMap.get("sum"))
    testMap.put("sum", a)
    println("adding a : "+testMap.get("sum"))
  }

}
