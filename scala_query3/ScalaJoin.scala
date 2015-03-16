import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object ScalaJoin {
  def main(args: Array[String]){
    val conf = new SparkConf().setAppName("Scala join test")
    val sc = new SparkContext(conf)
    val ranking = sc.textFile(args(0))
    val uservisit = sc.textFile(args(1))
    val rankingTable = ranking.map(line => line.split(","))
    val uservisitsTable = uservisit.map(line => line.split(","))


    //result.foreach(line => println(line.deep.mkString(" ")))
    result.take(args(3).toInt).foreach(line => println(line))
  }
}

