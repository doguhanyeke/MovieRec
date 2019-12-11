package edu.metu.ceng790.hw1

import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import scala.math.sqrt

object Part1 {
  case class Movie(
                    movieId: Int,
                    genres: Array[String],
                    tags: Array[(Int,Double)],
                    directors: Array[String],
                    writers: Array[String]
                  )

  def getLinks(sparkSession: SparkSession,filePrefix: String)= {
    val rawLinks = readAnything(sparkSession, filePrefix, "links.csv")
    val movieLensAndImdbIds = rawLinks.map( row => (row.getInt(1),row.getInt(0)))
    movieLensAndImdbIds
  }

  def getMovies(sparkSession: SparkSession,filePrefix: String)= {
    val rawMovies: RDD[Row] = readAnything(sparkSession, filePrefix, "movies.csv")
    val moviesWithTags = rawMovies.map( row => (row.getInt(0),row.getString(2).split("\\|")))
    moviesWithTags
  }

  def getGenomeScores(sparkSession: SparkSession,filePrefix: String) = {
    val rawGenomeScores: RDD[Row] = readAnything(sparkSession, filePrefix, "genome-scores.csv")
    val genomeScores = rawGenomeScores.map( row => (row.getInt(0), (row.getInt(1), row.getDouble(2))))
    genomeScores
  }

  def main(args: Array[String]): Unit = {
    var spark: SparkSession = null
    try {

      spark = SparkSession.builder().appName("Movie Rec using dataframes").config("spark.master", "local[*]").getOrCreate()
//      val filePrefix = "/home/utku/Desktop/Courses/bigD/ml-20m/"
      val filePrefix = "/home/dogu/Desktop/ceng790/ml-20m/"

      val originalFlickrMeta: DataFrame = readCrew(spark, filePrefix)
      val movies:RDD[(Int,Array[String])] = getMovies(spark,filePrefix)//collaborative_filtering.getMovies(spark, filePrefix)

      // YOUR CODE HERE
      originalFlickrMeta.createOrReplaceTempView("crewData")
      val dfCrew = spark.sql("SELECT * FROM crewData")
      val crew: RDD[(Int, (Array[String], Array[String]))] = dfCrew.rdd.map(row => {
        var directors:Array[String] = null
        var writers :Array[String] = null

        if(row.getString(1) == "\\N") {
          directors = new Array[String](0)
        } else {
          directors = row.getString(1).split(",")
        }

        if(row.getString(2) == "\\N") {
          writers = new Array[String](0)
        } else {
          writers = row.getString(2).split(",")
        }
        (row.getString(0).substring(2).toInt, (directors,writers))
      })

      val links = getLinks(spark, filePrefix)
      val moviesWithCrew = links.join(crew)//.map(imdb => (imdb._2._1,(imdb._2._2._1,imdb._2._2._2)))

      val mlindexed = moviesWithCrew.map(ele => (ele._2._1, ele._2._2))

      //reading genomes
      val genomeScores: RDD[(Int, Iterable[(Int, Double)])] = getGenomeScores(spark, filePrefix).groupByKey()

      // (directors,writers), (tagId,()
      val tmpJoin: RDD[(Int, ((Array[String], Array[String]), Iterable[(Int, Double)]))] = mlindexed.join(genomeScores)

      val movieAllFeatures = movies.join(tmpJoin)
      val allMovies = movieAllFeatures.map(m => Movie(m._1, m._2._1, m._2._2._2.toArray, m._2._2._1._1, m._2._2._1._2))

      //allMovies.take(10).foreach(printMovie)
      val mov1: Movie = allMovies.first()
      val mov2: Movie = allMovies.first()

      println("here")
      val simMeasure = compareTwoMovies(mov1, mov2)
      println("here2")
      println(simMeasure)

    }
  }

  private def getAllMoviesAndRatesOfUser(sparkSession: SparkSession, prefix: String, userId: Int, movies: RDD[Part1.Movie]) = {
    val ratings = Hw2Part1.getRatingRDD(sparkSession, prefix)
    val userToRating: RDD[(Int, Iterable[Rating])] = ratings.filter(_.user equals userId).groupBy(_.user)
    val movieIdsUserRated = ratings.map(_.product).collect()
    val moviesUserRated = movies.filter(m => movieIdsUserRated.contains(m.movieId)).map(m=>(m.movieId,m) )
    val movieIdToMovie = movies.map(m=>(m.movieId,m))

    val tmp: RDD[(Int, (Iterable[Rating], Movie))] = ratings.groupBy(_.product).join(movieIdToMovie)
    val userToMoviesAndRate: RDD[(Int, (Movie, Double))] = tmp.flatMap(x => {
      x._2._1.map(r => (r.user, (x._2._2, r.rating)))
    })
    userToMoviesAndRate
    //    val goodRDD = ratings.groupBy(_.user).map(pair => {
    //      val average = pair._2.map(_.rating).sum / pair._2.size
    //      pair._2.filter(r => r.rating >= average)/*.map(u => (u.user,u.product))*/
    //    }).flatMap(identity)

  }

  def userToMovieVec(user: RDD[(Int,(Part1.Movie,Double))]): Unit = {

  }

  def compareTwoMovies(movie1: Movie, movie2: Movie): Unit = {
    //movieId: Int,
    //genres: Array[String],
    //tags: Array[(Int,Double)],
    //directors: Array[String],
    //writers: Array[String]

    //Jaccard distance used for categorical data
    val genresScore = scoreFinderForTwoArrays(movie1.genres, movie2.genres)
    val directorScore = scoreFinderForTwoArrays(movie1.directors, movie2.directors)
    val writerScore = scoreFinderForTwoArrays(movie1.writers, movie2.writers)

    //cosine distance used for continuous data
//    val tagsScore = cosineSimilarity(movie1.tags, movie2.tags)
//    val score = genresScore + directorScore + writerScore + tagsScore
//    score
  }

  //HELPER FUNCTION
  //for categorical type Arrays
  def scoreFinderForTwoArrays(arr1: Array[String], arr2: Array[String]) : Double = {
    val intResult = arr1.intersect(arr2).length
    println(intResult)
    val uniResult = arr1.union(arr2).length
    println(uniResult)
    val tagsScore = intResult / uniResult.toDouble
    tagsScore
  }

  //HELPER FUNCTION
  //dot product
  def cosineSimilarity(arr1: Array[Double], arr2: Array[Double]): Double = {
    var index = 0
    val size = arr1.length - 1
    var result = 0.0
    var size1 = 0.0
    var size2 = 0.0
    for(index <- 0 to size){
      result += arr1(index) * arr2(index)
      size1 += arr1(index) * arr1(index)
      size2 += arr2(index) * arr2(index)
    }
    val res = result / (sqrt(size1) * sqrt(size2))
    res
  }


  private def printMovie(movie: Movie) = {
    print("Mid " + movie.movieId )
    print("\n\tdirectors: " + movie.directors.mkString(","))
    print("\n\twriters: " + movie.writers.mkString(","))
    print("\n\tgenres: " + movie.genres.mkString("|"))
    print("\n")
    //print("\n\ttags: ")
    //movie.tags.foreach(t => print("tagId: " + t._1 + ", rlvnce: " + t._2 + ","))
  }

  private def readCrew(spark: SparkSession, filePrefix: String) = {
    //   * movie identifier
    //   * writerlar arası sim func
    //   * sevmediklerimizden + katkılı sim func
    val customSchemaFlickrMeta = StructType(Array(
      StructField("movie_id", StringType, true),
      StructField("director_id", StringType, true),
      StructField("writer", StringType, true)))
    // datasets path

    val originalFlickrMeta = spark.sqlContext.read
      .format("csv")
      .option("delimiter", "\t")
      .option("header", "true")
      .schema(customSchemaFlickrMeta)
      .load(filePrefix + "data.tsv")
    originalFlickrMeta
  }

  def readAnything(sparkSession: SparkSession, filePrefix: String, fileName: String ) = {
    sparkSession.read.format("com.databricks.spark.csv").option("header", true).option("inferSchema", "true").load(filePrefix + fileName).rdd
  }
}