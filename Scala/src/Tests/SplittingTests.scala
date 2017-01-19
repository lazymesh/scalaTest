package src.Tests

import org.apache.spark.SparkContext

/**
  * Created by ramaharjan on 1/19/17.
  */
object SplittingTests {

  def main(args: Array[String]): Unit = {

  }

  def splitting(): Unit ={
    val textString = "hello^*~world"
    val splitText = textString.split("\\^\\*~")
    splitText.foreach(println)
  }

  def readAndSplit(file : String, sc : SparkContext): Unit ={
    val fileData = sc.textFile(file).cache()
    val lineData = fileData.map(x =>x.split("\\^\\*~"))
    val yourRdd = lineData.flatMap( arr => {
      val title = arr( 0 )
      val text = arr( 1 )
      val words = title.split( "\\^%~" )
      words.map( word => ( word ) )
    } )
    // RDD[ ( String ) ]

    // Now, if you want to print this...
    yourRdd.foreach( { case ( word ) => println( s"$word" ) } )
  }
}
