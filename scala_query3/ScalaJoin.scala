import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object ScalaJoin {
  def main(args: Array[String]){
    val conf = new SparkConf().setAppName("Scala-join-test")
    val sc = new SparkContext(conf)
    val ranking = sc.textFile(args(0))
    val uservisit = sc.textFile(args(1))
    val rankingTable = ranking.map(line => line.split(","))
    val uservisitsTable = uservisit.map(line => line.split(","))
    val format = new java.text.SimpleDateFormat("yyyy-MM-dd")
    val start_date = format.parse(args(2))
    val end_date = format.parse(args(3))
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
  }
}

