import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import scala.collection.Map
import org.apache.log4j.{Level, Logger}

object WordCount {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("SparkJoins").setMaster("local")
    val sc = new SparkContext(conf)

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val file = sc.textFile("src/resources/mytextfile.txt")

    val words = file.flatMap(_.split(" ")).map(word => (word,1)).reduceByKey(_+_).sortBy(_._2,false)

    words.foreach(println)


    sc.stop()
  }
}