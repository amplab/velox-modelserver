package edu.berkeley.veloxms

import edu.berkeley.veloxms.storage._
import com.typesafe.scalalogging._
import scala.util.Success
import scala.util.Failure
import scala.util.Try
import java.nio.ByteBuffer
import java.io.IOException

class MatrixFactorizationModel(
    val numFeatures: Int,
    val modelStorage: ModelStorage[FeatureVector],
    val averageUser: WeightVector) extends Model[Long, FeatureVector]
    with LazyLogging {

  /**
   * User provided implementation for the given model. Will be called
   * by Velox on feature cache miss.
   */
  def computeFeatures(data: Long): FeatureVector = {
    modelStorage.getFeatureData(data) match {
      case Success(features) => features
      case Failure(thrown) => {
        val msg = "Error computing features: " + thrown
        logger.warn(msg)
        throw new Exception(msg)
      }
    }
  }

  def deserializeInput(data: Array[Byte]) : Long = {
    ByteBuffer.wrap(data).getLong

  }

  def getFeatures(item: Long, cache: FeatureCache[Long]): FeatureVector = {
    featureCache.getItem(item) match {
      case Some(f) => f
      case None => {
        val f = model.computeFeatures(item)
        featureCache.addItem(item, f)
        f
      }
  }

}



