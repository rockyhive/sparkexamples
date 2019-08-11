package WordLength
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import scala.collection.Map
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import utilities.SparkUtilities

/**
  * Created by new user on 8/11/2019.
  */
object maxLength {
def main(args:Array[String]) :Unit = {
  val spark = SparkUtilities.getSparkSession(this.getClass.getName)
  val sconf = new SparkConf().setAppName("Max Length").setMaster("local")
  val sc = new SparkContext(sconf)

  val tfile = sc.textFile("src/resources/mytextfile.txt",3)
  println(tfile.getNumPartitions)
  val wordLengthMapSorted =  tfile.flatMap(_.split(" ")).map(word=> (word,word.length)).distinct().sortBy(_._2,false,2)
  println(wordLengthMapSorted.getNumPartitions)
  wordLengthMapSorted.foreach(println)







  +
  -
}
}
