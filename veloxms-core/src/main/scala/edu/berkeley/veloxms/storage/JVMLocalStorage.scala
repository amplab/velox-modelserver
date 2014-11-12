package edu.berkeley.veloxms.storage


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
class JVMLocalStorage (
    users: ConcurrentHashMap[Long, WeightVector],
    items: ConcurrentHashMap[Long, FeatureVector],
    ratings: ConcurrentHashMap[Long, ConcurrentHashMap[Long, Double]],
    val numFactors: Int) extends ModelStorage[FeatureVector] with Logging {


    def getFeatureData(itemId: Long): Try[FeatureVector] = {

        Option(items.get(itemId)).map(a => Success(a))
            .getOrElse(Failure[FeatureVector](
                new Throwable(s"$itemId not a valid item id")))
    }

    def getUserFactors(userId: Long): Try[WeightVector] = {

        Option(users.get(userId)).map(a => Success(a))
            .getOrElse(Failure[WeightVector](
                new Throwable(s"$userId New user, no existing user model")))
    }

    def getAllObservations(userId: Long): Try[Map[Long, Double]] = {

        Option(ratings.get(userId)).map(a => Success(a.toMap))
            .getOrElse((Success(new HashMap[Long, Double])))
    }

    def addScore(userId: Long, itemId: Long, score: Double) = {
      val userEntry = Option(ratings.get(userId)).getOrElse({
        val newMap = new ConcurrentHashMap[Long, Double]()
        ratings.put(userId, newMap)
        newMap
      })

      userEntry.put(itemId, score)
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
    ): JVMLocalStorage = {
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

    def apply(
        userFile: String,
        itemFile: String,
        ratingsFile: String,
        numFactors: Int): JVMLocalStorage = {
      new JVMLocalStorage(
        createUserModelFromFile(userFile),
        createItemModelFromFile(itemFile),
        createObservationsFromFile(ratingsFile),
        numFactors)
    }

    // wrapper for type signatures
    def createUserModelFromFile(file: String): ConcurrentHashMap[Long, WeightVector] = {
      logInfo(s"Reading USER model from $file")
      readModelFromFile(file) 
    }

    def createItemModelFromFile(file: String): ConcurrentHashMap[Long, FeatureVector] = {
      logInfo(s"Reading ITEM model from $file")
       readModelFromFile(file) 
    }

    // helper method for reading model from file
    def readModelFromFile(file: String): ConcurrentHashMap[Long, Array[Double]] = {
      var i = 0
      val map = new ConcurrentHashMap[Long, Array[Double]]
      Source.fromFile(file).getLines.foreach( (line) => {
        val splits = line.split(",")
        val key = splits(0).toLong
        val factors: Array[Double] = splits.slice(1, splits.size).map(_.toDouble)
        map.put(key, factors)
        if (i <= 20) { logInfo(s"key: $key") }
        i += 1
      })
      // map.foreach({ case (k, _) => logInfo(s"key: $k")})
      map
    }

    def createObservationsFromFile(file: String)
        : ConcurrentHashMap[Long, ConcurrentHashMap[Long, Double]] = {


        logInfo(s"Reading OBSERVATIONS from $file")
        val map = new ConcurrentHashMap[Long, ConcurrentHashMap[Long, Double]]
        val obs = Source.fromFile(file)
            .getLines
            .map( (line) => {
                val splits = line.split("\\s+")
                Observation(splits(0).toLong, splits(1).toLong, splits(2).toDouble)
            })
            .toList
            .groupBy(_.user)
            .map({ case (user, obs) => {
                val obsMap: ConcurrentHashMap[Long, Double] =
                    new ConcurrentHashMap(obs.map(r => (r.data, r.score)).toMap)
                map.put(user, obsMap)
                }
            })
        map
    }
}


