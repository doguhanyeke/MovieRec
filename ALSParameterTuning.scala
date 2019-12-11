package edu.metu.ceng790.hw1

import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

object ALSParameterTuning {

  val utku: Array[(Int,Double)] = Array((111161, 5),(68646, 5),(71562, 4),(468569, 5),(108052, 5),(167260, 5),(137523, 5),(120737, 5),(109830, 4.3),(80684, 4),(1375666, 4.7),(167261, 5),(133093, 5),(110413, 4),(816692, 3.9),(88763, 4),(103064, 2.5),(2582802, 5),(172495, 4),(1853728, 4),(910970, 5),(1345836, 5),(112573, 2),(75314, 2.5),(97576, 1),(372784, 5),(95016, 3.9),(347149, 3.5),(268978, 5),(993846, 4),(434409, 4),(1291584, 4),(266543, 4),(107290, 5),(2278388, 5),(88247, 3),(2015381, 4),(325980, 5))

  def imdbToMvId(param: Array[(Int,Double)]) = {
    val imdbIds = param.map(_._1)

  }

  def splitData(orig_df : RDD[Rating])= {
    val train: Double = 0.9
    val test: Double = 0.1
    val seperatedDFs = orig_df.randomSplit(Array(train,test))
    (seperatedDFs(0),seperatedDFs(1))
  }

  def mse(predictions:RDD[Rating], original:RDD[Rating]): Double = {
    val preds = predictions.map( x => ((x.user, x.product), x.rating))
    val orgs = original.map( x => ((x.user, x.product), x.rating))

    val join = orgs.join(preds)

    val result = join.map { case ((_, _), (r1, r2)) =>
      val err = r1 - r2
      err * err
    }.mean()
    result
  }
}
