package edu.berkeley.veloxms.models

import edu.berkeley.veloxms._
import edu.berkeley.veloxms.storage.BroadcastProvider
import org.apache.spark.rdd._
import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation.{ALS,Rating}
import edu.berkeley.veloxms.util._
class MatrixFactorizationModel(
    val modelName: String,
    val broadcastProvider: BroadcastProvider,
    val numFeatures: Int,
    val averageUser: WeightVector
  ) extends Model[Long] {

  val defaultItem: FeatureVector = Array.fill[Double](numFeatures)(0.0)

  val itemStorage = broadcast[Map[Long, FeatureVector]]("items")

  /**
   * User provided implementation for the given model. Will be called
   * by Velox on feature cache miss.
   */
  def computeFeatures(data: Long, version: Version): FeatureVector = {
    itemStorage.get(version).flatMap(_.get(data)) match {
      case Some(features) => features
      case None => {
        val msg = s"Features for item $data not found, defaulting to a 0-vector"
        logWarning(msg)
        defaultItem
      }
    }
  }

  /**
   * Retrains
   * @param observations
   * @param nextVersion
   * @return
   */
  override def retrainFeatureModelsInSpark(
      observations: RDD[(UserID, Long, Double)],
      nextVersion: Version): RDD[(UserID, FeatureVector)] = {
    val trainingData = observations.map(y => Rating(y._1.toInt, y._2.toInt, y._3))
    val iterations = 5
    val lambda = 1
    val model = ALS.train(trainingData, numFeatures, iterations, lambda)

    itemStorage.put(model.productFeatures.map(x => (x._1.toLong, x._2)).collect().toMap, nextVersion)
    model.productFeatures.map(x => (x._1.toLong, x._2))
  }
}
