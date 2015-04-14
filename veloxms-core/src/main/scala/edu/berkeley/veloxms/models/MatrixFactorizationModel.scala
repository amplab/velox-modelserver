package edu.berkeley.veloxms.models

import edu.berkeley.veloxms._
import edu.berkeley.veloxms.util.EtcdClient
import org.apache.spark.rdd._
import org.apache.spark.mllib.recommendation.{ALS,Rating}

class MatrixFactorizationModel(
    val name: String,
    val etcdClient: EtcdClient,
    val numFeatures: Int,
    val averageUser: WeightVector,
    val cacheResults: Boolean,
    val cacheFeatures: Boolean,
    val cachePredictions: Boolean
  ) extends Model[Long, FeatureVector] {

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
        val msg = s"Features for item $data not found"
        logWarning(msg)
        throw new NoSuchElementException(msg)
      }
    }
  }

  /**
   * Retrains
   * @param observations
   * @param nextVersion
   * @return
   */
  override protected def retrainItemFeaturesInSpark(observations: RDD[(UserID, Long, Double)], nextVersion: Version): RDD[(UserID, FeatureVector)] = {
    val trainingData = observations.map(y => Rating(y._1.toInt, y._2.toInt, y._3))
    val iterations = 5
    val lambda = 1
    val model = ALS.train(trainingData, numFeatures, iterations, lambda)

    itemStorage.put(model.productFeatures.map(x => (x._1.toLong, x._2)).collect().toMap, nextVersion)
    model.productFeatures.map(x => (x._1.toLong, x._2))
  }
}