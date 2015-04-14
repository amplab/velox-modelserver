
package edu.berkeley.veloxms.models

import java.util.Date
import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue}

import edu.berkeley.veloxms._
import edu.berkeley.veloxms.storage._
import edu.berkeley.veloxms.util._
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.rdd.RDD

import scala.collection.JavaConversions._
import scala.collection.mutable

// import org.codehaus.jackson.JsonNode
import com.fasterxml.jackson.databind.{ObjectMapper, JsonNode}
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

  val name: String
  val etcdClient: EtcdClient

  private var version: Version = new Date(0)
  def currentVersion: Version = version
  def useVersion(version: Version): Unit = {
    // TODO: Implement cache invalidation!
    broadcasts.foreach(_.cache(version))
    this.version = version
  }

  // TODO: Observations should be stored w/ Timestamps, and in a more relational format w/ persistence
  val observations: ConcurrentHashMap[UserID, mutable.Map[T, Double]] = new ConcurrentHashMap()
  val userWeights: ConcurrentHashMap[(UserID, Version), WeightVector] = new ConcurrentHashMap()

  val cacheResults: Boolean
  val cacheFeatures: Boolean
  val cachePredictions: Boolean

  val featureCache: FeatureCache[(T, Version), Array[Double]] =
      new FeatureCache[(T, Version), Array[Double]](cacheFeatures)

  val predictionCache: FeatureCache[(UserID, T, Version), Double] =
      new FeatureCache[(UserID, T, Version), Double](cachePredictions)

  private val partialResults = new PartialResultsCache()


  /** The number of features in this model.
   * Used for pre-allocating arrays/matrices
   */
  val numFeatures: Int

  /**
   * The default feature vector to use for an incomputable item
   */
  val defaultItem: FeatureVector

  /** Average user weight vector.
   * Used for warmstart for new users
   * TODO: SHOULD BE RETRAINED WHEN BULK RETRAINING!!!
   **/
  val averageUser: WeightVector

  // FIXME: Add some sort of Broadcast provider instead of hardcoding the EtcdBroadcast
  val broadcasts = new ConcurrentLinkedQueue[VersionedBroadcast[_]]()
  protected def broadcast[V](id: String): VersionedBroadcast[V] = {
    val b = new VersionedEtcdBroadcast[V](s"$name/$id", etcdClient)
    broadcasts.add(b)
    b
  }

  /**
   * User provided implementation for the given model. Will be called
   * by Velox on feature cache miss.
   */
  protected def computeFeatures(data: T, version: Version) : FeatureVector

  /**
   * Retrains 
   * @param observations
   * @param nextVersion
   * @return
   */
  protected def retrainFeatureModelsInSpark(observations: RDD[(UserID, T, Double)], nextVersion: Version): RDD[(T, FeatureVector)]

  // TODO: Make this much more efficient. Currently a very naive implementation
  // TODO: MAKE SURE THESE ARE CORRECT!!!
  private def retrainUserWeightsInSpark(itemFeatures: RDD[(T, FeatureVector)], observations: RDD[(UserID, T, Double)]): RDD[(UserID, WeightVector)] = {
    val obs = observations.map(x => (x._2, (x._1, x._3)))
    val featureRatings = itemFeatures.join(obs).map(x => (x._2._2._1, (x._2._1, x._2._2._2)))
    val ratingsByUserId = featureRatings.groupByKey()
    val lambda = UpdateMethods.lambda

    // TODO: File a bug in the spark closure cleaner?
    // Because w/o this line closure cleaner can't remove MatrixFactorizationModel (which isn't serializable) and the code breaks
    val k = numFeatures
    ratingsByUserId.map { case (id, ratings) => {
      val partialFeaturesSum = DenseMatrix.zeros[Double](k, k)
      val partialScoresSum = DenseVector.zeros[Double](k)

      for ((features, score) <- ratings) {
        val currentFeatures = new DenseVector(features) // Column Vector
        val product = currentFeatures * currentFeatures.t
        partialFeaturesSum += product
        partialScoresSum += currentFeatures * score
      }

      val regularization = DenseMatrix.eye[Double](k) * (lambda*k)
      partialFeaturesSum += regularization
      val newUserWeights = partialFeaturesSum \ partialScoresSum
      (id, newUserWeights.toArray)
    }}
  }

  // TODO: probably want to elect a leader to initiate the Spark retraining
  // once we are running a Spark cluster
  def retrainInSpark(sparkMaster: String, trainingDataDir: String, newModelsDir: String, nextVersion: Version) {
    // This is installation specific
    val sparkHome = "/root/spark-1.3.0-bin-hadoop1"
    logWarning("Starting spark context")
    val conf = new SparkConf()
        .setMaster(sparkMaster)
        .setAppName("VeloxOnSpark!")
        .setJars(SparkContext.jarOfObject(this).toSeq)
        .setSparkHome(sparkHome)

    val sc = new SparkContext(conf)

    // TODO: Have to make sure this trainingData contains observations from ALL nodes!!
    // TODO: This could be made better
    val trainingData: RDD[(UserID, T, Double)] = sc.objectFile(s"$trainingDataDir/*/*")

    val itemFeatures = retrainFeatureModelsInSpark(trainingData, nextVersion)
    val userWeights = retrainUserWeightsInSpark(itemFeatures, trainingData).map({
      case (userId, weights) => s"$userId, ${weights.mkString(", ")}"
    })


    userWeights.saveAsTextFile(newModelsDir + "/users")

    sc.stop()
    logInfo("Finished retraining new model")
  }

  /**
   * Velox implemented - fetch from local Tachyon partition
   *
   */
  private def getWeightVector(userId: Long, version: Version) : WeightVector = {
    val result: Option[Array[Double]] = Option(userWeights.get((userId, version)))
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
  private def getFeatures(item: T, version: Version): FeatureVector = {
    val features: Try[FeatureVector] = featureCache.getItem(item, version) match {
      case Some(f) => Success(f)
      case None => {
        Try(computeFeatures(item, version)).transform(
          (f) => {
            featureCache.addItem((item, version), f)
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

  def predict(uid: UserID, context: JsonNode, version: Version): Double = {
    val item: T = jsonMapper.treeToValue(context, classTag[T].runtimeClass.asInstanceOf[Class[T]])
    predictItem(uid, item, version)
  }

  private[this] def predictItem(uid: UserID, item: T, version: Version): Double = {
    val score = predictionCache.getItem((uid, item, version)) match {
      case Some(p) => p
      case None => {
        val features = getFeatures(item, version)
        val weightVector = getWeightVector(uid, version)
        var i = 0
        var accumScore = 0.0
        while (i < numFeatures) {
          accumScore += features(i) * weightVector(i)
          i += 1
        }
        predictionCache.addItem((uid, item, version), accumScore)
        accumScore
      }
    }
    score
  }

  def predictTopK(uid: Long, k: Int, context: JsonNode, version: Version): Array[T] = {
    // FIXME: There is probably some threshhold of k for which it makes more sense to iterate over the unsorted list
    // instead of sorting the whole list.
    val itemOrdering = new Ordering[T] {
      override def compare(x: T, y: T) = {
        -1 * (predictItem(uid, x, version) compare predictItem(uid, y, version))
      }
    }
    val candidateSet: Array[T] = jsonMapper.treeToValue(context, classTag[Array[T]].runtimeClass.asInstanceOf[Class[Array[T]]])
    Sorting.quickSort(candidateSet)(itemOrdering)

    candidateSet.slice(0, k)
  }

  def addObservation(uid: Long, context: JsonNode, score: Double, version: Version) {
    // TODO: ALWAYS add the new observation. partial results cache should depend on Version though!!
    (this, uid).synchronized {
      val item: T = jsonMapper.treeToValue(context, classTag[T].runtimeClass.asInstanceOf[Class[T]])
      val newWeights = addObservationInternal(uid, item, score, version, newData = true)
      userWeights.put((uid, version), newWeights)
    }
  }

  private def addObservationInternal(
      uid: Long,
      context: T,
      score: Double,
      version: Version,
      newData: Boolean = true): WeightVector = {

    val k = numFeatures
    // FIXME: This precomputed value may easily be wrong if keep swapping versions !!!!!!
    val precomputed = Option(partialResults.get((version, uid)))
    val partialFeaturesSum = precomputed.map(_._1).getOrElse(DenseMatrix.zeros[Double](k, k))
    val partialScoresSum = precomputed.map(_._2).getOrElse(DenseVector.zeros[Double](k))

    val allScores: Seq[(T, Double)] = if (newData) {
      val scores = observations.putIfAbsent(uid, mutable.Map())
      scores.put(context, score)
      scores.toSeq
    } else {
      observations.putIfAbsent(uid, mutable.Map()).toSeq
    }

    val newScores: Seq[(T, Double)] = if (precomputed == None) {
      allScores
    } else if (newData) {
      Seq(context -> score)
    } else {
      Seq()
    }

    val newItems: Seq[(FeatureVector, Double)] = newScores.map {
      case (c, s) => (getFeatures(c, version), s)
    }
    val (newWeights, newPartialResult) = UpdateMethods.updateWithBreeze(
      partialFeaturesSum, partialScoresSum, newItems)

    if (cacheResults) {
      partialResults.put((version, uid), newPartialResult)
    }
    newWeights
  }

  def writeUserWeights(weights: Map[UserID, WeightVector], version: Version): Unit = {
    weights.foreach(x => userWeights.put((x._1, version), x._2))
  }

  def getObservationsAsRDD(sc: SparkContext): RDD[(UserID, T, Double)] = {
    // TODO: This may not be safe if observation updates keep happening?
    val x = observations.toSeq.flatMap({ case (user, obs) =>
      obs.map { case (item, score) => (user, item, score) }
    })
    sc.parallelize(x)
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
      newRatings: TraversableOnce[(FeatureVector, Double)]):
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
    val newUserWeights = partialFeaturesSum \ partialScoresSum
    (newUserWeights.toArray, partialResult)
  }
}



