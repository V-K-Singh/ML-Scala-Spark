package spark.ml.dataset

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object ExploringDataset extends App{

  val conf = new SparkConf().setAppName("wordCount").setMaster("local[*]")

  val spark = SparkSession
    .builder()
    .appName("Exploring Dataset")
    .config(conf)
    .getOrCreate()

  var user_data = spark.read.textFile("E:\\big_data\\scala\\ml-scala-spark\\ml-100k\\u.user")
  println(user_data.first())
}
