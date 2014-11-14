package edu.berkeley.veloxms.storage


import java.nio.file.{Paths, Files}

import org.apache.commons.lang3.NotImplementedException
import edu.berkeley.veloxms.util.Logging
import tachyon.r.sorted.ClientStore
import tachyon.TachyonURI
import scala.util._
import java.io.IOException
import java.nio.ByteBuffer
import scala.collection.immutable.HashMap
import edu.berkeley.veloxms._
import edu.berkeley.veloxms.misc.Observation
import java.nio.ByteBuffer
import java.io.ByteArrayOutputStream
import edu.berkeley.veloxms.util.{VeloxKryoRegistrar, KryoThreadLocal, Logging}
import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConversions._
// import scala.concurrent.JavaConversions._
import scala.io.Source


/** Simple implementation of ModelStorage to avoid Tachyon
 * dependency. Should only be used for testing/debug purposes.
 */
class JVMLocalStorage[T, U] (
    users: ConcurrentHashMap[Long, WeightVector],
    items: ConcurrentHashMap[T, U],
    ratings: ConcurrentHashMap[Long, ConcurrentHashMap[T, Double]],
    val numFactors: Int) extends ModelStorage[T, U] with Logging {


    def getFeatureData(context: T): Try[U] = {

        Option(items.get(context)).map(a => Success(a))
            .getOrElse(Failure[U](
                new Throwable(s"$context not a valid item id")))
    }

    def getUserFactors(userId: Long): Try[WeightVector] = {

        Option(users.get(userId)).map(a => Success(a))
            .getOrElse(Failure[WeightVector](
                new Throwable(s"$userId New user, no existing user model")))
    }

    def getAllObservations(userId: Long): Try[Map[T, Double]] = {

        Option(ratings.get(userId)).map(a => Success(a.toMap))
            .getOrElse((Success(new HashMap[T, Double])))
    }

    def addScore(userId: Long, context: T, score: Double) = {
      val userEntry = Option(ratings.get(userId)).getOrElse({
        val newMap = new ConcurrentHashMap[T, Double]()
        ratings.put(userId, newMap)
        newMap
      })

      userEntry.put(context, score)
    }

  /**
   * Cleans up any necessary resources
   */
  override def stop() { }
}

object JVMLocalStorage extends Logging {


    def generateRandomData(
      totalNumUsers: Int,
      numItems: Int,
      numPartitions: Int,
      partition: Int,
      modelSize: Int = 50,
      maxScore: Int = 10
    ): JVMLocalStorage[Long, FeatureVector] = {
      val rand = new Random
      val itemMap = new ConcurrentHashMap[Long, FeatureVector]
      var item = 0
      while (item < numItems) {
        itemMap.put(item, randomArray(rand, modelSize))
        item += 1
      }
      logInfo("Generated item data")

      // val sizeOfPart = totalNumUsers / numPartitions
      val userMap = new ConcurrentHashMap[Long, WeightVector]
      val obsMap = new ConcurrentHashMap[Long, ConcurrentHashMap[Long, Double]]
      // var user = sizeOfPart * partition
      var user = 0
      while (user < totalNumUsers) {
        if (user % numPartitions == partition) {
          userMap.put(user, randomArray(rand, modelSize))
          val userObsMap = new ConcurrentHashMap[Long, Double]
          var j = 0
          // generate observations for 10% of the items
          while (j < numItems / 10) {
            userObsMap.put(j, rand.nextDouble * maxScore)
            j += 1
          }
          obsMap.put(user, userObsMap)
          // if (user % 1000 == 0) {
          //   logInfo(s"Generated data for $user out of max ${sizeOfPart *(partition + 1)} users")
          //
          // }
        }
        user += 1
      }
      logInfo("Generated user data")
      logInfo("Done generating random data")
      new JVMLocalStorage(userMap, itemMap, obsMap, modelSize)
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

    def apply[T, U](
        userFile: String,
        itemFile: String,
        ratingsFile: String,
        numFactors: Int): JVMLocalStorage[T, U] = {
      new JVMLocalStorage[T, U](
        createUserModelFromFile(userFile),
        createItemModelFromFile[T, U](itemFile),
        createObservationsFromFile[T](ratingsFile),
        numFactors)
    }

    // wrapper for type signatures
    def createUserModelFromFile(file: String): ConcurrentHashMap[Long, WeightVector] = {
      logInfo(s"Reading USER model from $file")
      readModelFromFile[Long, WeightVector](file)
    }

    def createItemModelFromFile[T, U](file: String): ConcurrentHashMap[T, U] = {
      logInfo(s"Reading ITEM model from $file")
      readModelFromFile[T, U](file)
    }

    // helper method for reading model from file
    def readModelFromFile[T, U](file: String): ConcurrentHashMap[T, U] = {
      var i = 0
      val kryo = KryoThreadLocal.kryoTL.get
      val rawBytes = ByteBuffer.wrap(Files.readAllBytes(Paths.get(file)))
      kryo.deserialize(rawBytes).asInstanceOf[ConcurrentHashMap[T, U]]
    }

    def createObservationsFromFile[T](file: String)
        : ConcurrentHashMap[Long, ConcurrentHashMap[T, Double]] = {


        logInfo(s"Reading OBSERVATIONS from $file")
        readModelFromFile(file)
    }
}


