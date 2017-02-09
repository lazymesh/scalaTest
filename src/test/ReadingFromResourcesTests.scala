package test

import org.scalatest.{BeforeAndAfterEach, FunSuite}
import scala.io.Source

/**
  * Created by ramaharjan on 2/1/17.
  */
class ReadingFromResourcesTests extends FunSuite with BeforeAndAfterEach {
  override def beforeEach() {

  }

  override def afterEach() {

  }

  test("testing to read from resources"){
    val text = Source.fromInputStream(getClass.getResourceAsStream("/validation_eligibility.jobcfg"))
    text.foreach(print)
  }

  test("updating a hashmap"){
    val array = Seq("1,2,3,2,3,4","a,d,f,s,a,e")
    val splittedArray = array.map(row => {
      println(row)
      row.split(",")
    }).map(word => {
      println(word(0))

    })

  }
}
