
package edu.berkeley.veloxms

import com.typesafe.scalalogging.LazyLogging
import scala.util.Success
import scala.util.Failure

trait Model[T] extends LazyLogging {

  /** The number of features in this model.
   * Used for pre-allocating arrays/matrices
   */
  val numFeatures: Int

  val modelStorage: ModelStorage

  /** Average user weight vector.
   * Used for warmstart for new users
   */
  val averageUser: Array[Double]

  /**
   * User provided implementation for the given model. Will be called
   * by Velox on feature cache miss.
   */
  def computeFeatures(data: T): Array[Double]

  /**
   * Velox implemented - fetch from local Tachyon partition
   */
  final def getWeightVector(userId: Long) : Array[Double] = {
    modelStorage.getUserFactors(userId) match {
      case Success(u) => u
      case Failure(thrown) => {
        logger.warn("User weight failure: " + thrown)
        averageUser
      }
    }
  }


}



