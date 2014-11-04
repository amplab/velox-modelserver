package edu.berkeley.veloxms.storage

import edu.berkeley.veloxms.WeightVector
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

  users.put(TachyonUtils.long2ByteArr(validUId), kryo.serialize(vector).array)

  ratings.put(TachyonUtils.long2ByteArr(validUId), kryo.serialize(map).array)

  users.close
  items.close
  ratings.close

  val storage = new RocksStorage(usersPath, itemsPath, ratingsPath, 50)

  // these tests tests both getUserFactors and getFeatureData since they use the exact same logic
  behavior of "RocksStorage#getUserFactors"

  it should "fail if the key doesn't exist" in {
    val result = storage.getUserFactors(invalidUId)

    assert (result === null || result.isFailure)
  }

  it should "return the WeightVector if the key exists" in {
    val result = storage.getUserFactors(validUId)

    assert (result.isSuccess)
    assert (result.get === vector)
  }

  behavior of "RocksStorage#getAllObservations"

  // this is testing is deserialization still works with maps
  it should "return the Map if the key exists" in {
    val result = storage.getAllObservations(validUId)

    assert (result.isSuccess)
    assert (result.get === map)
  }

  behavior of "RocksStorage#addScore"

  it should "correctly add a score for an existing user" in {
    storage.addScore(validUId, 10L, 10.0)

    val result = storage.getAllObservations(validUId)
    assert(result.isSuccess)

    val map = result.get

    assert (map.keySet.contains(10L))
    assert (map.get(10L).get === 10.0)
  }

  it should "correctly add a score for a previously unrecorded user" in {
    storage.addScore (3l, 10L, 10.0)

    val result = storage.getAllObservations(3L)
    assert(result.isSuccess)

    val map = result.get

    assert (map.keySet.contains(10L))
    assert (map.get(10L).get === 10.0)
  }
}
