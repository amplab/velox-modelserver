package edu.berkeley.veloxms.models

import edu.berkeley.veloxms.{UserID, Version, FeatureVector}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import pipelines.Transformer
import com.fasterxml.jackson.databind.JsonNode
import edu.berkeley.veloxms.storage.BroadcastProvider

import scala.reflect.ClassTag

/**
 * A model that uses a Keystone [[Transformer]] to featurize the context
 */
abstract class KeystoneModel[T : ClassTag](
    override val modelName: String,
    override val broadcastProvider: BroadcastProvider,
    override val jsonConfig: Option[JsonNode])
  extends Model[T](modelName, broadcastProvider, jsonConfig) {
  private val modelBroadcast = broadcast[Transformer[T, FeatureVector]]("model")

  override def computeFeatures(data: T, version: Version): FeatureVector = {
    modelBroadcast.get(version) match {
      case Some(transformer) => transformer(data)
      case None => {
        logWarning(s"No model trained for $modelName, defaulting to a 0-vector")
        Array.fill[Double](numFeatures)(0.0)
      }
    }
  }

  override def retrainFeatureModelsInSpark(observations: RDD[(UserID, T, Double)], nextVersion: Version): RDD[(T, FeatureVector)] = {
    // Set up all the contexts
    val nextModel = fit(observations.sparkContext)
    modelBroadcast.put(nextModel, nextVersion)

    val features = observations.map(_._2).distinct()
    features.zip(nextModel(features))
  }

  def fit(sc: SparkContext): Transformer[T, FeatureVector]
}
