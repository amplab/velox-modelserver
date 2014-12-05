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
import java.nio.ByteBuffer
import java.io.ByteArrayOutputStream
import java.util.concurrent.ConcurrentHashMap
import edu.berkeley.veloxms.util.{VeloxKryoRegistrar, KryoThreadLocal}

// class TachyonStorage[U: ClassTag] (
class TachyonStorage[K, V] (
                         tachyonMaster: String,
                         storeName: String) extends ModelStorage[K, V] with Logging {

  val store = TachyonUtils.getStore(tachyonMaster, storeName) match {
    case Success(s) => s
    case Failure(f) => throw new RuntimeException(
      s"Couldn't initialize use model: ${f.getMessage}")
  }

  val storeCache = new ConcurrentHashMap[K, V]

  logInfo("got tachyon stores")

  /**
   * Cleans up any necessary resources
   */
  override def stop() {}

  override def put(kv: (K, V)): Unit = storeCache.put(kv._1, kv._2)

  override def get(key: K): Option[V] = {
    val result = Option(storeCache.get(key)).getOrElse({
      val rawBytes = ByteBuffer.wrap(store.get(StorageUtils.toByteArr(key)))
      val kryo = KryoThreadLocal.kryoTL.get
      kryo.deserialize(rawBytes).asInstanceOf[V]
    })
    Some(result)
  }

  // def getAllObservations(userId: Long): Try[Map[Long, Double]] = {
  //
  //   val rawBytes = ByteBuffer.wrap(ratings.get(StorageUtils.long2ByteArr(userId)))
  //   val kryo = KryoThreadLocal.kryoTL.get
  //   val result = kryo.deserialize(rawBytes).asInstanceOf[HashMap[Long, Double]]
  //
  //   Success(TachyonUtils.mergeObservations(Option(result), Option(observationsCache.get(userId))))
  //     //
  //     // val result = for {
  //     //     rawBytes <- Try(ratings.get(TachyonUtils.long2ByteArr(userId)))
  //     //     array <- Try(rawBytes.unpickle[Map[Long, Double]])
  //     // } yield array
  //     // result
  // }
}



object TachyonUtils {

  def getStore(tachyon: String, kvloc: String): Try[ClientStore] = {
    val url = s"$tachyon/$kvloc"
    Try(ClientStore.getStore(new TachyonURI(url))) match {
      case f: Failure[ClientStore] => f
      case Success(u) => if (u == null) {
        Failure(new RuntimeException(s"Tachyon store $url not found"))
      } else {
        Success(u)
      }
    }
  }

  def mergeObservations[T]( tachyonMap: Option[HashMap[T, Double]],
                         cacheEntry: Option[HashMap[T, Double]]): HashMap[T, Double] = {
    if (!cacheEntry.isDefined) {
      tachyonMap.getOrElse({
        new HashMap[T, Double]
      })
    } else if (!tachyonMap.isDefined) {
      cacheEntry.get
    } else {
      var result = tachyonMap.get

      for (iid <- cacheEntry.get.keys)  {
        if (result.keySet.contains(iid)) { // if there is an entry, then remove it
          result = result - iid
        }

        val cacheEntryVal: Double = cacheEntry.get.get(iid) match {
          case Some(cacheEntryVal) => cacheEntryVal
          case None => throw new RuntimeException("iid in key set not found on get")
        }

        result = result + (iid -> cacheEntryVal)
      }

      result
    }
  }

}


