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
    val text = Source.fromInputStream(getClass.getResourceAsStream("/validation_eligibility.jobcfg")).mkString
    println(text)
  }
}
