package edu.metu.ceng790.hw1

import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._

import scala.util.Random

object collaborative_filtering {

  def main(args: Array[String]): Unit = {

    var spark: SparkSession = null
    try {

      spark = SparkSession.builder().appName("Flickr using dataframes").config("spark.master", "local[*]").getOrCreate()
      //val filePrefix = "/home/utku/Desktop/Courses/bigD/ml-20m/"
      val filePrefix = "/home/dogu/Desktop/ceng790/ml-20m/"

      // Movie id to movie title
      val movies: RDD[(Int, String)] = getMovies(spark, filePrefix)
      val ratings = Hw2Part1.getRatingRDD(spark,filePrefix)
      ratings.cache()

      val mostRatedMovies: Array[Int] = ratings.map(x => (x.product, 1)).reduceByKey(_ + _).sortBy(_._2, false).take(200).map(_._1)
     // movies.filter(m=>mostRatedMovies.contains(m._1)).foreach(println)

      // 40 random movies from most rated 200 movies
      val randomMovies = get40(ratings)

      val userRated = elicitateRatings(movies.filter(mov=> randomMovies.contains(mov._1)))
      val additionalRDD = spark.sparkContext.parallelize(userRated.map(p=>Rating(Integer.MAX_VALUE,p._1,p._2)).toList)
      val union = Hw2Part1.normalizeRatingData(ratings.union(additionalRDD))
      val model = Hw2Part1.trainModel(union, 12, 20, 0.01)

      val predictions = model.predict(movies.map(m => (Integer.MAX_VALUE,m._1)))
      val forPrint = predictions.sortBy(_.rating, false).take(20)
      forPrint.foreach(println)
      val top20Recom = predictions.sortBy(_.rating, false).take(20).map(_.product)
      movies.filter(mov=> top20Recom.contains(mov._1)).foreach(println)
//      predictions.sortBy(_.rating, false).take(20).foreach(println)

    } catch {
      case e: Exception => throw e
    } finally {
      spark.stop()
    }
    println("done")
  }

  def get40(ratings: RDD[Rating]) = {
    // Similar to word count, take 200 most rated movie ids
    val mostRatedMovies: Array[Int] = ratings.map(x => (x.product, 1)).reduceByKey(_ + _).sortBy(_._2, false).take(200).map(_._1)
    // Get 40 in a randomized manner
    getRandom40(mostRatedMovies)
  }

  def getMovies(spark: SparkSession, filePrefix: String): RDD[(Int, String)] = {
    loadMovies(spark, filePrefix).map(x => (x.getInt(0), x.getString(1)))
  }

  def loadMovies(spark: SparkSession, filePrefix: String) = {
    spark.read.format("com.databricks.spark.csv").option("header", true).option("inferSchema", "true").load(filePrefix + "movies.csv").rdd
  }

  def getRandom40(mostRatedMovies: Array[Int]) = {
    Random.shuffle(mostRatedMovies.toList).take(40)
  }

  def elicitateRatings(selectedMovies : RDD[(Int,String)]) = {
    val collected = selectedMovies.collect()
    var ratedMovies: Map[Int,Double] = Map[Int,Double]()
    collected.foreach( movie => {
      Console.println("Rate the movie " + movie._2)
      val rate = Console.readDouble()
      Console.println()
      ratedMovies += (movie._1 -> rate)
    })
    ratedMovies
  }
}
