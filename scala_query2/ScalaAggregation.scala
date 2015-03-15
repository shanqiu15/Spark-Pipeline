import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object ScalaAggregation {
  def main(args: Array[String]){
    val conf = new SparkConf().setAppName("Scala aggregation test")
    val sc = new SparkContext(conf)
    val lines = sc.textFile(args(0))
    val uservisitsTable = lines.map(line => line.split(","))
    val pairs = uservisitsTable.map(row => (row(0).substring(args(1).toInt, args(2).toInt), row(3).toDouble))
    val result = pairs.reduceByKey((x, y) => x + y)
    //result.foreach(line => println(line.deep.mkString(" ")))
    result.take(args(3).toInt).foreach(line => println(line))
  }
}

