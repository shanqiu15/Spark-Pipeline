/*
*
* sbt clean compile test package
* ```
*  name := "scala-scan-test"
*  version := "0.0.1"
*  scalaVersion := "2.11.6"
*  // additional libraries
*  libraryDependencies ++= Seq(
*  "org.apache.spark" %% "spark-core" % "1.2.0" % "provided"
*  )
* ```
*/
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object ScalaQuery {
  def main(args: Array[String]){
    val conf = new SparkConf().setAppName("Scala scan test")
    val sc = new SparkContext(conf)
    val lines = sc.textFile(args(0))
    val rankingTable = lines.map(line => line.split(","))
    val scan_query_start = System.nanoTime
    val result = rankingTable.filter(row => row(1).toInt > args(1).toInt)
    //result.foreach(line => println(line.deep.mkString(" ")))
    result.take(args(2).toInt).foreach(line => println(line.deep.mkString(" ")))
    val scan_query_time = (System.nanoTime - scan_query_start)/1e9
    println("Scan query running time: "+ scan_query_time +"s")
  }
}
