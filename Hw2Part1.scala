package edu.metu.ceng790.hw1

import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD


object Hw2Part1 {

  def main(args: Array[String]): Unit = {

    val inpStr = Console.readLine("\nEnter Training params\n").split(",")
    val rank = inpStr(0).toInt
    val lambda = inpStr(1).toDouble
    val iterations = inpStr(2).toInt

    var spark: SparkSession = null
    try {

      spark = SparkSession.builder().appName("Flickr using dataframes").config("spark.master", "local[*]").getOrCreate()
      //val filePrefix = "/home/utku/Desktop/Courses/bigD/ml-20m/"
      val filePrefix = "/home/dogu/Desktop/ceng790/ml-20m/"
      // Read csv to rdd
      val wholeset: RDD[Rating] = getRatingRDD(spark,filePrefix)//df.rdd.map( row => Rating(row.getInt(0), row.getInt(1), row.getDouble(2)))

      // Normalize data
      val wholesetNormalized = normalizeRatingData(wholeset)

      // Split data to train and test
      val (training,testRatings) = ALSParameterTuning.splitData(wholesetNormalized)

      training.cache()
      testRatings.cache()

      val model: MatrixFactorizationModel = trainModel(training, rank, iterations, lambda)

      val testInp = testRatings.map(x => (x.user,x.product))
      val preds = model.predict(testInp)

      val mse = ALSParameterTuning.mse(preds, testRatings)
      println(s"Mean Squared Error = $mse")
    } catch {
          case e: Exception => throw e
    } finally {
      spark.stop()
    }
    println("done")
  }

  def normalizeRatingData(wholeset: RDD[Rating]) = {
    wholeset.groupBy(_.user).map(pair => {
      val average = pair._2.map(_.rating).sum / pair._2.size
      val res: Iterable[Rating] = pair._2.map(rate => {
        Rating(rate.user, rate.product, rate.rating - average)
      })
      res
    }).flatMap(identity)
  }

  def trainModel(ratings: RDD[Rating], rank: Int, iterations: Int, lambda: Double) = {
    /*val userToRatings: RDD[(Int, Iterable[Rating])] = ratings.groupBy(_.user)

    val ratingsN: RDD[Rating] = userToRatings.map(pair => {
      val average = pair._2.map(_.rating).sum / pair._2.size
      val yettings: Iterable[Rating] = pair._2.map(rate => {
        Rating(rate.user, rate.product, rate.rating - average)
      })
      yettings
    }).flatMap(identity)*/

    val model = ALS.train(ratings, rank, iterations, lambda)
    model
  }

  def getRatingRDD(spark: SparkSession, filePrefix: String): RDD[Rating] = {
    val originalFlickrMeta =   spark.sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .option("quote", "\"") //escape the quotes
      .option("ignoreLeadingWhiteSpace", true) // escape space before your data
      .load(filePrefix + "ratings.csv")
    originalFlickrMeta.createOrReplaceTempView("ratings")

    val df = spark.sql("SELECT userId, movieId, rating FROM ratings")
    df.rdd.map( row => Rating(row.getInt(0), row.getInt(1), row.getDouble(2)))
  }

}
