package edu.metu.ceng790.hw1

import edu.metu.ceng790.hw1.collaborative_filtering.elicitateRatings
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object nearestneighbors {

//  private val fp = "/home/utku/Desktop/Courses/bigD/ml-20m/"
  private val fp = "/home/dogu/Desktop/ceng790/ml-20m/"

  def main(args: Array[String]): Unit = {
    var spark = SparkSession.builder().appName("Flickr using dataframes").config("spark.master", "local[*]").getOrCreate()

    // Ratings that are above their average for each user
    val goodRatings: RDD[Rating] = getGoodRatings(spark)
    // Get movie tags
    val movieTags = getMovieTags(spark)
    // Create user vectors
    val userVectors: RDD[(Int, Map[String, Int])] = getUserTaste(goodRatings,movieTags)

    // Get random 40 movies from most rated 200 movies
    val forty = collaborative_filtering.get40(Hw2Part1.getRatingRDD(spark,fp))
    val allMovies = collaborative_filtering.getMovies(spark,fp)
    // Let user rate them
    var userRated = collaborative_filtering.elicitateRatings(allMovies.filter(mov=> forty.contains(mov._1)))
    var userRateSum = 0.0
    userRated.foreach(arg => userRateSum += arg._2)
    userRateSum = userRateSum / userRated.size
    //Get only the ones user truely liked
    userRated = userRated.filter(arg => arg._2 >= userRateSum)
    // Create a user profile from rated movies
    val userTags = getUserTaste(spark.sparkContext.parallelize(userRated.map(o=> Rating(Int.MaxValue,o._1,o._2)).toList),movieTags)

    // Knn stuff, find similar users, recommend movies liked by them
    val intermediate = userTags.collect()
    val similarUsers = knn(intermediate(0),userVectors,4)
    println("similarusers size: " + similarUsers.size)
    val justUserIds = similarUsers.map(_._1)
    val recommMovieIds = goodRatings.filter( k=> justUserIds.contains(k.user)).map(_.product).collect()
    val recommMovies = collaborative_filtering.getMovies(spark,fp).filter( m => recommMovieIds.contains(m._1)).map(_._2).collect()
    println("Recommended Movies are:")
    recommMovies.foreach(println)
  }

  def getGoodRatings(spark: SparkSession) = {
    val ratings = Hw2Part1.getRatingRDD(spark, fp)
    val goodRDD = ratings.groupBy(_.user).map(pair => {
        val average = pair._2.map(_.rating).sum / pair._2.size
        pair._2.filter(r => r.rating >= average)/*.map(u => (u.user,u.product))*/
      }).flatMap(identity)
    goodRDD
  }

  def getMovieTags(spark: SparkSession) = {
    val movs = collaborative_filtering.loadMovies(spark,fp)
    val result: RDD[(Int, Array[String])] = movs.map(x => (x.getInt(0), x.getString(2).split("\\|")))
    result
  }

  def getUserTaste(goodRatings:RDD[Rating], movieTags:RDD[(Int,Array[String])]) = {
    val mola= goodRatings.map(gr => (gr.product,gr)).join(movieTags).map(arg=>(arg._2._1.user,arg._2._2))
    //mola.flatMap( y=> y._2.map(t => (y._1,))

    val wordCountLike: RDD[((Int, String), Int)] = mola.flatMap(t => {
      t._2.map(z => ((t._1, z), 1))
    })

    val uf: RDD[(Int, Iterable[((Int, String), Int)])] = wordCountLike.reduceByKey(_+_).groupBy(_._1._1)

    val oldu: RDD[(Int, Map[String, Int])] = uf.map(user => {
      val map: Map[String, Int] = user._2.map(param => (param._1._2, param._2)).toMap
      (user._1, map)
    })
    oldu
  }

  def userSim(u1 :(Int, Map[String, Int]), u2 : (Int, Map[String, Int])) = {
    val commonKeys: Set[String] = u1._2.keys.toSet.union(u2._2.keys.toSet)
    var nominator = 0.0
    commonKeys.foreach(commonKey => {
      nominator += u1._2.getOrElse(commonKey,0) * u2._2.getOrElse(commonKey,0)
    })
    var u1DeNom : Double = 0
    u1._2.foreach(num => u1DeNom = num._2 * num._2)
    u1DeNom = Math.sqrt(u1DeNom)

    var u2DeNom : Double = 0
    u2._2.foreach(num => u2DeNom = num._2 * num._2)
    u2DeNom = Math.sqrt(u2DeNom)

    nominator / (u1DeNom * u2DeNom)
  }

  def knn(userProfile :(Int, Map[String, Int]), userVectors: RDD[(Int, Map[String, Int])], k:Int) = {
    val tuples = userVectors.map(uv => {
      val similarity: Double = userSim(userProfile, uv)
      (uv._1, similarity)
    }).sortBy(_._2).take(k)
    tuples
  }
}
