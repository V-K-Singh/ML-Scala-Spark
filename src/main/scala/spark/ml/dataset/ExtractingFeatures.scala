package spark.ml.dataset

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.Rating

object ExtractingFeatures extends App{

  val conf = new SparkConf().setAppName("wordCount").setMaster("local")

  val userFile="E:\\big_data\\scala\\ml-scala-spark\\ml-100k\\u.data"
  val spark = SparkSession
    .builder()
    .appName("Exploring Dataset")
    .config(conf)
    .getOrCreate()

  case class UserData(userId :Int, itemId :Int, rating:Double)

  import spark.implicits._
  val rawData: Dataset[String] = spark.read.textFile(userFile)
//  rawData.show()
  var rawRatings = rawData.map(_.split("\t")).map(x => UserData(x(0).toInt,x(1).toInt,x(2).toDouble))
//    rawRatings.foreach(r => println(r))
val ratings = rawRatings.map { userData => Rating(userData.userId, userData.itemId, userData.rating) }

//  println(ratings.first())
  /* This returns a MatrixFactorizationModel object, which contains the user and item
  factors in the form of an RDD of (id, factor) pairs. These are called userFeatures
    and productFeatures, respectively.
    */
   val model = ALS.train(ratings.toJavaRDD, 50, 10, 0.01)

  println(model.userFeatures.count())

}

