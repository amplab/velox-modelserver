package edu.berkeley.veloxms.models

import edu.berkeley.veloxms._

import scala.reflect.ClassTag
import edu.berkeley.veloxms.storage._
import edu.berkeley.veloxms.util.Logging


abstract class PipelinesModel[T: ClassTag](
    val numFeatures: Int,
    val model: (T) => (FeatureVector),
    val modelStorage: ModelStorage[T, FeatureVector],
    val averageUser: WeightVector,
    val conf: VeloxConfiguration) extends Model[T, FeatureVector] with Logging {

    val defaultItem: FeatureVector = Array.fill[Double](numFeatures)(0.0)

  /**
   * User provided implementation for the given model. Will be called
   * by Velox on feature cache miss.
   */
  def computeFeatures(data: T): FeatureVector = {
    model(data)
  }

  def retrainInSpark(sparkMaster: String = conf.sparkMaster) {}
}
