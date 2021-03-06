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
    val lines = sc.textFile("hdfs://localhost:8020/tmp/benchmark/text/tiny/rankings")
    val rankingTable = lines.map(line => line.split(","))
    val result = rankingTable.filter(row => row(1).toInt > 50)
    result.foreach(line => println(line.deep.mkString(" ")))
  }
}
