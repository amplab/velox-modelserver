
package edu.berkeley.veloxms.models

import edu.berkeley.veloxms._
import edu.berkeley.veloxms.storage._
import edu.berkeley.veloxms.util.Logging
// import org.codehaus.jackson.JsonNode
import com.fasterxml.jackson.databind.JsonNode
import org.jblas.{Solve, DoubleMatrix}
import breeze.linalg._

import scala.reflect._
import scala.util.{Failure, Success, Try}
import scala.util.Sorting


/**
 * Model interface
 * @tparam T The scala type of the item, deserialized from Array[Byte]
 * We defer deserialization to the model interface to encapsulate everything
 * the user must implement into a single class.
 * @tparam U The type of per-item data being stored in the
 * KV store
 */
abstract class Model[T:ClassTag, U] extends Logging {

  val cacheResults: Boolean

  val cacheFeatures: Boolean

  val cachePredictions: Boolean

  val featureCache: FeatureCache[T, Array[Double]] =
      new FeatureCache[T, Array[Double]](cacheFeatures)

  val predictionCache: FeatureCache[(Long, T), Double] =
      new FeatureCache[(Long, T), Double](cachePredictions)

  private val partialResults = new PartialResultsCache()


  /** The number of features in this model.
   * Used for pre-allocating arrays/matrices
   */
  val numFeatures: Int

  /**
   * The default feature vector to use for an incomputable item
   */
  val defaultItem: FeatureVector

  /**
   * Interface to the storage backend. Allows model implementers
   * to access the storage system if needed for computing features,
   * user weights.
   */
  val userStorage: ModelStorage[Long, WeightVector]
  val observationStorage: ModelStorage[Long, Map[T, Double]]

  def getObservationsAsCSV: List[String] = {
    val entries = observationStorage.getEntries
    entries.flatMap({ case (user, obs) => {
        obs.map { case (item, score) => s"$user, $item, $score" }
      }
    }).toList
  }


  /** Average user weight vector.
   * Used for warmstart for new users
   */
  val averageUser: WeightVector

  /**
   * User provided implementation for the given model. Will be called
   * by Velox on feature cache miss.
   */
  protected def computeFeatures(data: T) : FeatureVector

  // TODO: probably want to elect a leader to initiate the Spark retraining
  // once we are running a Spark cluster
  def retrainInSpark(sparkMaster: String, trainingDataDir: String, newModelsDir: String)

  /**
   * Velox implemented - fetch from local Tachyon partition
   *
   */
  private def getWeightVector(userId: Long) : WeightVector = {
    val result: Option[Array[Double]] = userStorage.get(userId)
    result match {
      case Some(u) => u
      case None => {
        logWarning(s"User weight not found, userID: $userId")
        averageUser
      }
    }
  }

  // TODO(crankshaw) fix the error handling here to return default item features
  // TODO(crankshaw) the error handling here is fucked
  private def getFeatures(item: T): FeatureVector = {
    val features: Try[FeatureVector] = featureCache.getItem(item) match {
      case Some(f) => Success(f)
      case None => {
        Try(computeFeatures(item)).transform(
          (f) => {
            featureCache.addItem(item, f)
            Success(f)
          },
          (t) => {
            logWarning("Couldn't compute item features, using default of 0")
            Success(defaultItem)
          })
      }
    }
    features.get.clone()
  }

  def predict(uid: Long, context: JsonNode): Double = {
    val item: T = jsonMapper.treeToValue(context, classTag[T].runtimeClass.asInstanceOf[Class[T]])
    predictItem(uid, item)
  }

  private[this] def predictItem(uid: Long, item: T): Double = {
    val score = predictionCache.getItem((uid, item)) match {
      case Some(p) => p
      case None => {
        val features = getFeatures(item)
        val weightVector = getWeightVector(uid)
        var i = 0
        var accumScore = 0.0
        while (i < numFeatures) {
          accumScore += features(i) * weightVector(i)
          i += 1
        }
        predictionCache.addItem((uid, item), accumScore)
        accumScore
      }
    }
    score
  }

  def predictTopK(uid: Long, k: Int, context: JsonNode): Array[T] = {
    // FIXME: There is probably some threshhold of k for which it makes more sense to iterate over the unsorted list
    // instead of sorting the whole list.
    val itemOrdering = new Ordering[T] {
      override def compare(x: T, y: T) = {
        -1 * (predictItem(uid, x) compare predictItem(uid, y))
      }
    }
    val candidateSet: Array[T] = jsonMapper.treeToValue(context, classTag[Array[T]].runtimeClass.asInstanceOf[Class[Array[T]]])
    Sorting.quickSort(candidateSet)(itemOrdering)

    candidateSet.slice(0, k)
  }

  def addObservation(uid: Long, context: JsonNode, score: Double) {
    (this, uid).synchronized {
      val item: T = jsonMapper.treeToValue(context, classTag[T].runtimeClass.asInstanceOf[Class[T]])
      val newWeights = addObservationInternal(uid, item, score, true)
    }
  }

  private def addObservationInternal(
      uid: Long,
      context: T,
      score: Double,
      newData: Boolean = true): WeightVector = {

    val k = numFeatures
    val precomputed = Option(partialResults.get(uid))
    val partialFeaturesSum = precomputed.map(_._1).getOrElse(DenseMatrix.zeros[Double](k, k))
    val partialScoresSum = precomputed.map(_._2).getOrElse(DenseVector.zeros[Double](k))



    val allScores: Map[T, Double] = if (newData) {
      val scores = observationStorage.get(uid).getOrElse(Map()) + (context -> score)
      observationStorage.put(uid -> scores)
      scores
    } else {
      observationStorage.get(uid).getOrElse(Map())
    }
    val newScores: Map[T, Double] = if (precomputed == None) {
      allScores
    } else if (newData) {
      Map(context -> score)
    } else {
      Map()
    }
    val newItems: Map[T, FeatureVector] = newScores.map {
      case (context, _) => (context, getFeatures(context))
    }
    val (newWeights, newPartialResult) = UpdateMethods.updateWithBreeze(
      partialFeaturesSum, partialScoresSum, newItems, newScores, k)

    if (cacheResults) {
      partialResults.put(uid, newPartialResult)
    }
    newWeights
  }


  def precomputePartialSums(users: Range) {
    for (uid <- users) {
      // val fakeSums = (DenseMatrix.rand[Double](numFeatures, numFeatures),
      //   DenseVector.rand[Double](numFeatures))
      // partialResults.put(uid, fakeSums)
      addObservationInternal(uid, null.asInstanceOf[T], 0.0, false)
    }
  }



}




object UpdateMethods {

  val lambda = 1.0


  /**
   * Use breeze for matrix ops, does no caching or anything smart.
   */
  def updateWithBreeze[T:ClassTag](
      partialFeaturesSum: DenseMatrix[Double],
      partialScoresSum: DenseVector[Double],
      newItems: Map[T, FeatureVector],
      newScores: Map[T, Double],
      k: Int): (WeightVector, (DenseMatrix[Double], DenseVector[Double])) = {



    var i = 0

    val observedItems = newItems.keys.toList

    while (i < observedItems.size) {
      val currentItem = observedItems(i)
      // TODO error handling
      val currentFeaturesArray = newItems.get(currentItem) match {
        case Some(f) => f
        case None => throw new Exception(
          s"Missing features in online update -- item: $currentItem")
      }
      // column vector
      val currentFeatures = new DenseVector(currentFeaturesArray)
      val product = currentFeatures * currentFeatures.t
      partialFeaturesSum += product

      val obsScore = newScores.get(currentItem) match {
        case Some(o) => o
        case None => throw new Exception(
          s"Missing rating in online update -- item: $currentItem")
      }

      partialScoresSum += currentFeatures * obsScore
      i += 1
    }


    val partialResult = (partialFeaturesSum.copy, partialScoresSum.copy)

    val regularization = DenseMatrix.eye[Double](k) * (lambda*k)
    partialFeaturesSum += regularization
    val newUserWeights = partialFeaturesSum \ partialScoresSum
    (newUserWeights.toArray, partialResult)

  }


  /**
   * The original implementation. Uses jblas for matrix ops, does no
   * caching or anything smart.
   */
  // def updateJBLASNaive[T:ClassTag](allItemFeatures: Map[T, FeatureVector],
  //                       allObservationScores: Map[T, Double], k: Int): WeightVector = {
  //
  //
  //   val itemFeaturesSum = DoubleMatrix.zeros(k, k)
  //   val itemScoreProductSum = DoubleMatrix.zeros(k)
  //
  //   var i = 0
  //
  //   val observedItems = allItemFeatures.keys.toList
  //
  //   while (i < observedItems.size) {
  //     val currentItemId = observedItems(i)
  //     // TODO error handling
  //     val currentFeaturesArray = allItemFeatures.get(currentItemId) match {
  //       case Some(f) => f
  //       case None => throw new Exception(
  //         s"Missing features in online update -- item: $currentItemId")
  //     }
  //     val currentFeatures = new DoubleMatrix(currentFeaturesArray)
  //     val product = currentFeatures.mmul(currentFeatures.transpose())
  //     itemFeaturesSum.addi(product)
  //
  //     val obsScore = allObservationScores.get(currentItemId) match {
  //       case Some(o) => o
  //       case None => throw new Exception(
  //         s"Missing rating in online update -- item: $currentItemId")
  //
  //     }
  //     itemScoreProductSum.addi(currentFeatures.mul(obsScore))
  //     i += 1
  //
  //   }
  //
  //   // TODO: There should be no dependency on MatrixFactorizationModel here
  //   val regularization = DoubleMatrix.eye(k).muli(lambda*k)
  //   itemFeaturesSum.addi(regularization)
  //   val newUserWeights = Solve.solve(itemFeaturesSum, itemScoreProductSum)
  //   newUserWeights.toArray()
  //
  // }

}



