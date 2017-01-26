package src.Tests

/**
  * Created by ramaharjan on 1/20/17.
  */
object JSONParserTest {

  def jsonParser(): Unit ={
    import scala.util.parsing.json._

    val parsed = JSON.parseFull("""{"Name":"abc", "age":10}""")
    println(parsed)
  }
}
