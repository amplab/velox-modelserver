package edu.berkeley.veloxms.storage

import org.scalatest._
import scala.collection.immutable.HashMap

class TachyonStorageSpec extends FlatSpec with Matchers {
  val simpleMap = collection.immutable.HashMap(0L -> 1.0, 1L -> 2.0, 2L -> 3.0)

  val largerMap = collection.immutable.HashMap()

  behavior of "observation merging"

  it should "return tachyonMap if cacheEntry is empty"  in {
    val result = TachyonUtils.mergeObservations(simpleMap, new HashMap[Long, Double]())

    assert (result.equals(simpleMap))
  }

  it should "not fail if tachyonMap is null" in {
    val result = TachyonUtils.mergeObservations(null, simpleMap)

    assert (result.equals(simpleMap))
  }

  it should "correctly override values when there are matching keys" in {
    val cacheEntry = collection.immutable.HashMap(0L -> 2.0)
    val result = TachyonUtils.mergeObservations(simpleMap, cacheEntry)

    assert (result.get(0L) == Some(2.0))
  }
}
