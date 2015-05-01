package edu.berkeley.veloxms.util

import breeze.linalg.{DenseMatrix, DenseVector}
import edu.berkeley.veloxms._
import org.apache.spark.rdd.RDD

object UserWeightUpdateMethods extends Serializable {
  val lambda = 1.0

  /**
   * Naive Implementation
   * TODO: Optimize this bulk calculation & Make this much more efficient. Currently a very naive implementation
   * TODO: MAKE SURE THESE ARE CORRECT!!!
   */
  def calculateMultiUserWeights(
      ratings: RDD[(UserID, FeatureVector, Double)],
      numFeatures: Int,
      lambda: Double = lambda)
    : RDD[(UserID, WeightVector)] = {
    val ratingsByUserId = ratings.map(x => (x._1, (x._2, x._3))).groupByKey()
    ratingsByUserId.map { case (id, singleUserRatings) => {
      val newUserWeights = calculateSingleUserWeights(singleUserRatings, numFeatures, lambda)._1
      (id, newUserWeights)
    }}
  }
  
  /**
   * Use breeze for matrix ops, does no caching or anything smart.
   */
  def calculateSingleUserWeights(
      ratings: TraversableOnce[(FeatureVector, Double)],
      numFeatures: Int,
      lambda: Double = lambda):
  (WeightVector, (DenseMatrix[Double], DenseVector[Double])) = {

    val emptyFeaturesSum = DenseMatrix.zeros[Double](numFeatures, numFeatures)
    val emptyScoresSum = DenseVector.zeros[Double](numFeatures)

    calculateSingleUserWeightsFromPartialSums(emptyFeaturesSum, emptyScoresSum, ratings, lambda)
  }

  /**
   * Use breeze for matrix ops, does no caching or anything smart.
   */
  def calculateSingleUserWeightsFromPartialSums(
      partialFeaturesSum: DenseMatrix[Double],
      partialScoresSum: DenseVector[Double],
      newRatings: TraversableOnce[(FeatureVector, Double)],
      lambda: Double = lambda):
  (WeightVector, (DenseMatrix[Double], DenseVector[Double])) = {

    for ((features, score) <- newRatings) {
      val currentFeatures = new DenseVector(features) // Column Vector
      val product = currentFeatures * currentFeatures.t
      partialFeaturesSum += product
      partialScoresSum += currentFeatures * score
    }

    val partialResult = (partialFeaturesSum.copy, partialScoresSum.copy)
    val k = partialScoresSum.length

    val regularization = DenseMatrix.eye[Double](k) * (lambda*k)
    partialFeaturesSum += regularization
    // TODO: Don't break when it's nonsingular (e.g. featuresSum + regularization = 0)
    val newUserWeights = partialFeaturesSum \ partialScoresSum
    (newUserWeights.toArray, partialResult)
  }
}
