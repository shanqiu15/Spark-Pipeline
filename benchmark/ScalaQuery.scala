/*
*
* sbt clean compile test package
* ```
*  name := "benchmark_scala"
*  version := "0.0.1"
*  scalaVersion := "2.11.6"
*  // additional libraries
*  libraryDependencies ++= Seq(
*  "org.apache.spark" %% "spark-core" % "1.2.0" % "provided"
*  )
* ```
* 
*  sbt clean package
*
*  $SPARK_HOME/bin/spark-submit --class ScalaQuery target/scala-2.11/benchmark_scala_2.11-0.0.1.jar amplab/text/tiny/rankings 50 10 amplab/text/tiny/uservisits 0 9 10 1980-01-01 1980-04-01
*/
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object ScalaQuery {
  def main(args: Array[String]){
    val conf = new SparkConf().setAppName("Scala scan test")
    val sc = new SparkContext(conf)
    val ranking_lines = sc.textFile(args(0))
    val rankingTable = ranking_lines.map(line => line.split(","))

    //Scan
    val scan_query_start = System.nanoTime
    val scan_result = rankingTable.filter(row => row(1).toInt > args(1).toInt)
    //result.foreach(line => println(line.deep.mkString(" ")))
    scan_result.take(args(2).toInt).foreach(line => println(line.deep.mkString(" ")))
    val scan_query_time = (System.nanoTime - scan_query_start)/1e9

    //Aggregation
    val uservisit_lines = sc.textFile(args(3))
    val uservisitsTable = uservisit_lines.map(line => line.split(","))
    val aggregation_query_start = System.nanoTime
    val pairs = uservisitsTable.map(row => (row(0).substring(args(4).toInt, args(5).toInt), row(3).toDouble))
    val aggregation_result = pairs.reduceByKey((x, y) => x + y)
    //result.foreach(line => println(line.deep.mkString(" ")))
    aggregation_result.take(args(6).toInt).foreach(line => println(line))
    val aggregation_query_time = (System.nanoTime - aggregation_query_start)/1e9


    //Join
    val join_query_start = System.nanoTime
    val format = new java.text.SimpleDateFormat("yyyy-MM-dd")
    val start_date = format.parse(args(7))
    val end_date = format.parse(args(8))
    val validDate = uservisitsTable.filter(line => format.parse(line(2)).compareTo(start_date) >= 0 && format.parse(line(2)).compareTo(end_date) <= 0)
    val rankingPairs = rankingTable.map(line => (line(0), line))
    val uservisitPairs = validDate.map(line => (line(1), line))
    val joinTable = uservisitPairs.join(rankingPairs)
    //x._2._1(0):sourceIP  x._2._2(1):pageRank   x._2._1(3):adRevenue
    val sourceIPAsKey = joinTable.map(x => (x._2._1(0), (x._2._2(1).toDouble, x._2._1(3).toDouble)))
    val groupSourceIP = sourceIPAsKey.mapValues(x => (x, 1)).reduceByKey((x, y) => ((x._1._1 + y._1._1 , x._1._2 + y._1._2), x._2 + y._2))
    val groupResult = groupSourceIP.map(x => (x._2._1._2, (x._2._1._1/x._2._2, x._1)))
    val result = groupResult.sortByKey(false)
    println(result.take(1)(0)._2._2, result.take(1)(0)._1, result.take(1)(0)._2._1)
    val join_query_time = (System.nanoTime - aggregation_query_start)/1e9

    println("Scan query running time: "+ scan_query_time +"s")
    println("Aggregation query running time: "+ aggregation_query_time +"s")
    println("Join query running time: "+ join_query_time +"s")
  }
}

