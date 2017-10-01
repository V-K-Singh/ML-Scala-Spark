package spark.ml.dataset

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions._

object ExploringDataset extends App{

  val conf = new SparkConf().setAppName("wordCount").setMaster("local")

  val userFile="E:\\big_data\\scala\\ml-scala-spark\\ml-100k\\u.user"
  val spark = SparkSession
    .builder()
    .appName("Exploring Dataset")
    .config(conf)
    .getOrCreate()

  val id       = StructField("id",       DataTypes.IntegerType)
  val age    = StructField("age",    DataTypes.IntegerType)
  val gender   = StructField("gender",   DataTypes.StringType)
  val occupation    = StructField("occupation",    DataTypes.StringType)
  val zipcode = StructField("zipcode", DataTypes.IntegerType)

  val fields = Array(id, age, gender, occupation, zipcode)
  val schema = StructType(fields)
  var user_data = spark
    .read
    .format("csv")
    .option("header", "false") //reading the headers
    .option("delimiter","|")
    .option("mode", "DROPMALFORMED")
      .schema(schema)
    .load(userFile).persist(StorageLevel.DISK_ONLY)
//  println(user_data.first())

  val num_user = user_data.count()
  var num_gender = user_data.select("gender").distinct().count()

  val  num_occupations = user_data.select("occupation").distinct().count()
  val num_zipcodes = user_data.select("zipcode").distinct().count()
  println(s"num users $num_user, num_gender $num_gender  num_occupations $num_occupations num_zipcodes $num_zipcodes")
}
