package edu.berkeley.veloxms.storage

import edu.berkeley.veloxms.{FeatureVector, WeightVector}
import edu.berkeley.veloxms.util.KryoThreadLocal
import org.scalatest._
import org.rocksdb.{Options, RocksDB}

class RocksStorageSpec extends FlatSpec with Matchers {
  val usersPath = "/tmp/users"
  val itemsPath = "/tmp/items"
  val ratingsPath = "/tmp/ratings"

  var users = RocksDB.open(new Options().setCreateIfMissing(true), usersPath)
  val items = RocksDB.open(new Options().setCreateIfMissing(true), itemsPath)
  val ratings = RocksDB.open(new Options().setCreateIfMissing(true), ratingsPath)

  val vector: WeightVector = Array(1.0, 2.0, 3.0)
  val map = scala.collection.immutable.HashMap(0L -> 1.0, 1L -> 2.0, 2L -> 3.0)
  val validUId = 1L
  val invalidUId = 2L

  val kryo = KryoThreadLocal.kryoTL.get

  users.put(StorageUtils.toByteArr(validUId), kryo.serialize(vector).array)

  ratings.put(StorageUtils.toByteArr(validUId), kryo.serialize(map).array)

  users.close
  items.close
  ratings.close

  val itemStorage = new RocksStorage[Long, FeatureVector](itemsPath)
  val userStorage = new RocksStorage[Long, WeightVector](usersPath)
  val ratingStorage = new RocksStorage[Long, Map[Long, Double]](ratingsPath)

  // these tests tests both getUserFactors and getFeatureData since they use the exact same logic
  behavior of "RocksStorage#getUserFactors"

  it should "fail if the key doesn't exist" in {
    val result = userStorage.get(invalidUId)

    assert (result === null || result.isEmpty)
  }

  it should "return the WeightVector if the key exists" in {
    val result = userStorage.get(validUId)

    assert (result.isDefined)
    assert (result.get === vector)
  }

  behavior of "RocksStorage#getAllObservations"

  // this is testing is deserialization still works with maps
  it should "return the Map if the key exists" in {
    val result = ratingStorage.get(validUId)

    assert (result.isDefined)
    assert (result.get === map)
  }

  behavior of "RocksStorage#addScore"

  it should "correctly add a score for an existing user" in {
    val allObservationScores = ratingStorage.get(validUId).getOrElse(Map()) + (10L -> 10.0)
    ratingStorage.put(validUId -> allObservationScores)

    val result = ratingStorage.get(validUId)
    assert(result.isDefined)

    val map = result.get

    assert (map.keySet.contains(10L))
    assert (map.get(10L).get === 10.0)
  }

  it should "correctly add a score for a previously unrecorded user" in {
    val allObservationScores = ratingStorage.get(3l).getOrElse(Map()) + (10L -> 10.0)
    ratingStorage.put(3l -> allObservationScores)

    val result = ratingStorage.get(3L)
    assert(result.isDefined)

    val map = result.get

    assert (map.keySet.contains(10L))
    assert (map.get(10L).get === 10.0)
  }
}
