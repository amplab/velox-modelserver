package edu.berkeley.veloxms.storage


import java.nio.file.{Paths, Files}

import scala.util._
import edu.berkeley.veloxms._
import java.nio.ByteBuffer
import edu.berkeley.veloxms.util.{KryoThreadLocal, Logging}
import java.util.concurrent.ConcurrentHashMap

/** Simple implementation of ModelStorage to avoid Tachyon
 * dependency. Should only be used for testing/debug purposes.
 */
class JVMLocalStorage[K, V] (store: ConcurrentHashMap[K, V]) extends ModelStorage[K, V] with Logging {
  /**
   * Cleans up any necessary resources
   */
  override def stop() { }

  override def put(kv: (K, V)): Unit = store.put(kv._1, kv._2)

  override def get(key: K): Option[V] = {
    if (store.containsKey(key)) Some(store.get(key)) else None
  }
}

object JVMLocalStorage extends Logging {

  def generateRandomObservationData(totalNumUsers: Int, numItems: Int, numPartitions: Int, partition: Int, maxScore: Int): JVMLocalStorage[Long, Map[Long, Double]] = {
    var rand = new Random
    val obsMap = new ConcurrentHashMap[Long, Map[Long, Double]]
    var user = 0
    while (user < totalNumUsers) {
      if (user % numPartitions == partition) {
        // generate observations for 10% of the items
        val userObsMap = (0 until (numItems / 10)).map(x => (x.toLong, rand.nextDouble * maxScore)).toMap
        obsMap.put(user, userObsMap)
      }
      user += 1
    }
    logInfo("Generated observation data")
    new JVMLocalStorage(obsMap)
  }

  def generateRandomUserData(totalNumUsers: Int, numPartitions: Int, partition: Int, modelSize: Int): JVMLocalStorage[Long, WeightVector] = {
    // val sizeOfPart = totalNumUsers / numPartitions
    val rand = new Random
    val userMap = new ConcurrentHashMap[Long, WeightVector]
    // var user = sizeOfPart * partition
    var user = 0
    while (user < totalNumUsers) {
      if (user % numPartitions == partition) {
        userMap.put(user, randomArray(rand, modelSize))
      }
      user += 1
    }
    logInfo("Generated user data")
    new JVMLocalStorage(userMap)
  }

  def generateRandomItemData(numItems: Int, modelSize: Int): JVMLocalStorage[Long, FeatureVector] = {
    val rand = new Random
    val itemMap = new ConcurrentHashMap[Long, FeatureVector]
    var item = 0
    while (item < numItems) {
      itemMap.put(item, randomArray(rand, modelSize))
      item += 1
    }
    logInfo("Generated item data")
    new JVMLocalStorage(itemMap)
  }

  private def randomArray(rand: Random, size: Int) : Array[Double] = {
    val arr = new Array[Double](size)
    var i = 0
    while (i < size) {
      arr(i) = rand.nextGaussian
      i += 1
    }
    arr
  }

  def apply[T, U](file: String): JVMLocalStorage[T, U] = {
    new JVMLocalStorage[T, U](readModelFromFile(file))
  }

  // helper method for reading model from file
  def readModelFromFile[T, U](file: String): ConcurrentHashMap[T, U] = {
    var i = 0
    val kryo = KryoThreadLocal.kryoTL.get
    val rawBytes = ByteBuffer.wrap(Files.readAllBytes(Paths.get(file)))
    kryo.deserialize(rawBytes).asInstanceOf[ConcurrentHashMap[T, U]]
  }
}


