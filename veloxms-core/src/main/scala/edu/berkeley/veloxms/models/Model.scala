
package edu.berkeley.veloxms.models

import java.util.Date
import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue}

import edu.berkeley.veloxms._
import edu.berkeley.veloxms.storage._
import edu.berkeley.veloxms.util._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.JavaConversions._
import scala.reflect._
import scala.util.Sorting
import com.fasterxml.jackson.databind.JsonNode


/**
 * Model interface
 */
abstract class Model[T: ClassTag](
    val modelName: String,
    val broadcastProvider: BroadcastProvider,
    val jsonConfig: Option[JsonNode])
  extends Logging {

  private var version: Version = new Date(0).getTime
  def currentVersion: Version = version
  def useVersion(version: Version): Unit = {
    broadcasts.foreach(_.cache(version))
    this.version = version
  }

  // TODO: Observations should be stored w/ Timestamps, and in a more relational format w/ persistence
  val observations: ConcurrentHashMap[UserID, ConcurrentHashMap[T, Double]] = new ConcurrentHashMap()
  val userWeights: ConcurrentHashMap[(UserID, Version), WeightVector] = new ConcurrentHashMap()

  /** The number of features in this model.
   * Used for pre-allocating arrays/matrices
   */
  def numFeatures: Int

  def jsonToInput(context: JsonNode): T = {
    val item: T = fromJson[T](context)
    item
  }

  def jsonArrayToInput(context: JsonNode): Array[T] = {
    val items: Array[T] = fromJson[Array[T]](context)
    items
  }


  val broadcasts = new ConcurrentLinkedQueue[VersionedBroadcast[_]]()
  protected def broadcast[V: ClassTag](id: String): VersionedBroadcast[V] = {
    val b = broadcastProvider.get[V](s"$modelName/$id")
    broadcasts.add(b)
    b
  }

  /** Average user weight vector.
   * Used for warmstart for new users
   * TODO: SHOULD BE RETRAINED WHEN BULK RETRAINING!!!
   **/
  val averageUser = broadcast[WeightVector]("avg_user")

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
  def retrainFeatureModelsInSpark(
      observations: RDD[(UserID, T, Double)],
      nextVersion: Version): RDD[(T, FeatureVector)]

  final def retrainUserWeightsInSpark(itemFeatures: RDD[(T, FeatureVector)], observations: RDD[(UserID, T, Double)]): RDD[(UserID, WeightVector)] = {
    val obs = observations.map(x => (x._2, (x._1, x._3)))
    val ratings = itemFeatures.join(obs).map(x => (x._2._2._1, x._2._1, x._2._2._2))
    UserWeightUpdateMethods.calculateMultiUserWeights(ratings, numFeatures)
  }

  /**
   *
   */
  private def getWeightVector(userId: Long, version: Version) : WeightVector = {
    val defaultAverage = Array.fill[Double](numFeatures)(1.0)
    val result: Option[Array[Double]] = Option(userWeights.get((userId, version)))
    result match {
      case Some(u) => u
      case None => {
        logWarning(s"User weight not found, userID: $userId")
        averageUser.get(version).getOrElse(defaultAverage)
      }
    }
  }

  def predict(uid: UserID, item: T, version: Version): Double = {
    println(item.getClass)
    val features = computeFeatures(item, version)
    val weightVector = getWeightVector(uid, version)
    var i = 0
    var score = 0.0
    while (i < numFeatures) {
      score += features(i) * weightVector(i)
      i += 1
    }
    score
  }

  def predictTopK(uid: UserID, k: Int, candidateSet: Array[T], version: Version): Array[T] = {
    // FIXME: There is probably some threshhold of k for which it makes more sense to iterate over the unsorted list
    // instead of sorting the whole list.
    val itemOrdering = new Ordering[T] {
      override def compare(x: T, y: T) = {
        -1 * (predict(uid, x, version) compare predict(uid, y, version))
      }
    }

    Sorting.quickSort(candidateSet)(itemOrdering)
    candidateSet.slice(0, k)
  }

  final def addObservation(uid: UserID, item: T, score: Double) {
    observations.putIfAbsent(uid, new ConcurrentHashMap())
    val scores = observations.get(uid)
    scores.put(item, score)
  }

  final def addObservations(observations: TraversableOnce[(UserID, T, Double)]): Unit = {
    observations.foreach(o => addObservation(o._1, o._2, o._3))
  }

  final def getObservationsAsRDD(sc: SparkContext): RDD[(UserID, T, Double)] = {
    val x = observations.flatMap({ case (user, obs) =>
      obs.map { case (item, score) => (user, item, score) }
    }).toArray
    if (x.length > 0) {
      sc.parallelize(x)
    } else {
      // This is done because parallelize of an empty seq errors and Spark EmptyRDD is private
      sc.parallelize[(UserID, T, Double)](Seq(null)).filter(_ => false)
    }
  }

  final def updateUser(uid: UserID, version: Version): Unit = {
    val ratings = Some(observations.get(uid)).getOrElse(new ConcurrentHashMap()).map {
      case (c, s) => (computeFeatures(c, version), s)
    }

    val (newWeights, newPartialResult) = UserWeightUpdateMethods.calculateSingleUserWeights(ratings, numFeatures)
    userWeights.put((uid, version), newWeights)
  }

  final def updateUsers(uids: TraversableOnce[UserID], version: Version): Unit = {
    uids.foreach(uid => updateUser(uid, version))
  }

  final def writeUserWeights(weights: Map[UserID, WeightVector], version: Version): Unit = {
    weights.foreach(x => userWeights.put((x._1, version), x._2))
  }
}
