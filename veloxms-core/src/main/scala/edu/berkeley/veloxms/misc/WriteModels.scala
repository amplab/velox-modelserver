package edu.berkeley.veloxms.misc


import org.apache.commons.io.IOUtils
import org.apache.commons.lang3.SerializationUtils
import tachyon.TachyonURI
import tachyon.Pair
import tachyon.r.sorted.ClientStore
import tachyon.r.sorted.{Utils => TUtils}
import edu.berkeley.veloxms.storage.TachyonUtils
import scala.collection.immutable.TreeMap
import scala.io.Source


/**
 * Utility for writing existing models in plaintext on the
 * local filesystem to Spark. Used only to setup environment for
 * testing purposes.
 */
class WriteModels(
  itemsTachyonDest: String,
  usersTachyonDest: String,
  obsTachyonDest: String,
  itemsLocal: String,
  usersLocal: String,
  obsLocal: String) {


  // Read MatrixFactorizationModel
  def readModel(modelLoc: String): SortedMap[Array[Byte], Array[Byte]] {
    val model = Source.fromFile(modelLoc).getLines.map( (line) => {
      val splits = line.split(",")
      val key = splits(0).toLong
      val factors: Array[Double] = splits.slice(1, splits.size).map(_.toDouble)
      (TachyonUtils.long2ByteArr(key), SerializableUtils.serialize(factors))
    })

    val sortedModel = TreeMap(model)(ByteOrdering)
  }

  def writeMapToTachyon(map: TreeMap[Array[Byte], Array[Byte]], loc: String) {
    val partition = 0
    ClientStore store = ClientStore.createStore(new TachyonURI(loc))
    store.createPartition(partition)
    map.foreach( {case (key, value) => store.put(partition, key, value) })
    store.closePartition(partition)
  }


}


// Use tachyon sort ordering
object ByteOrdering extends Ordering[Array[Byte]] {
  def compare(a: Array[Byte], b: Array[Byte]) = TUtils.compare(a, b)
}




