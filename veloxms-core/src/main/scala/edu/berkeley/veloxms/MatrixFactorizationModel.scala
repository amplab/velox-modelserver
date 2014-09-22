package edu.berkeley.veloxms

import edu.berkeley.veloxms.storage._
import com.typesafe.scalalogging._
import scala.util.Success
import scala.util.Failure
import scala.util.Try
import java.nio.ByteBuffer
import java.io.IOException

class MatrixFactorizationModel(numFeatures: Int,
    modelStorage: ModelStorage[Array[Double]]) extends Model[Long, Array[Double]]
    with LazyLogging {

  /**
   * User provided implementation for the given model. Will be called
   * by Velox on feature cache miss.
   */
  def computeFeatures(data: Long): Array[Double] = {
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
}



