
package edu.berkeley.veloxms

import com.typesafe.scalalogging._
import scala.util.Success
import scala.util.Failure
import scala.util.Try
import edu.berkeley.veloxms.storage._

trait Model[T, U] extends LazyLogging {

  /** The number of features in this model.
   * Used for pre-allocating arrays/matrices
   */
  val numFeatures: Int

  val modelStorage: ModelStorage[U]

  /** Average user weight vector.
   * Used for warmstart for new users
   */
  val averageUser: Array[Double]

  /**
   * User provided implementation for the given model. Will be called
   * by Velox on feature cache miss.
   */
  def computeFeatures(data: T) : Array[Double]

  /** Deserialize object representation from raw bytes to
   * the type of expected
   */
  def deserializeItem(data: Array[Byte]) : T

  /**
   * Velox implemented - fetch from local Tachyon partition
   */
  final def getWeightVector(userId: Long) : Array[Double] = {
    val result: Try[Array[Double]] = modelStorage.getUserFactors(userId)
    result match {
      case Success(u) => u
      case Failure(thrown) => {
        logger.warn("User weight failure: " + thrown)
        averageUser
      }
    }
  }


}



