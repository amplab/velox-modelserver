package edu.berkeley.veloxms.storage


import org.apache.commons.lang3.NotImplementedException
import edu.berkeley.veloxms.util.Logging
import tachyon.r.sorted.ClientStore
import tachyon.TachyonURI
import scala.util._
import java.io.IOException
// import java.util.HashMap
import java.nio.ByteBuffer
// import com.typesafe.scalalogging._
// import scala.collection.mutable
import scala.collection.immutable.HashMap
import edu.berkeley.veloxms._
import java.nio.ByteBuffer
import java.io.ByteArrayOutputStream
import java.util.concurrent.ConcurrentHashMap
import edu.berkeley.veloxms.util.{VeloxKryoRegistrar, KryoThreadLocal}
// import scala.pickling._
// import binary._

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

  // def serializeArray(arr: Array[Double]): Array[Byte] = {
  //   val buffer = ByteBuffer.allocate
  // }


  /*
  Message bytes are:
  8 bytes: request ID; high-order bit is request or response boolean
  N bytes: serialized message
   */
  // def serializeMessage(requestId: RequestId, msg: Any, isRequest: Boolean): ByteBuffer = {
  //   val buffer = ByteBuffer.allocate(4096)
  //   var header = requestId & ~(1L << 63)
  //   if(isRequest) header |= (1L << 63)
  //   buffer.putLong(header)
  //   //val kryo = VeloxKryoRegistrar.getKryo()
  //   val kryo = KryoThreadLocal.kryoTL.get
  //   val result = kryo.serialize(msg,buffer)
  //   //VeloxKryoRegistrar.returnKryo(kryo)
  //   result.flip
  //   result
  // }

  // def deserializeMessage(bytes: ByteBuffer): (Any, RequestId, Boolean) = {
  //   val headerLong = bytes.getLong()
  //   val isRequest = (headerLong >>> 63) == 1
  //   val requestId = headerLong & ~(1L << 63)
  //   // TODO: use Kryo serializer pool instead
  //   //val kryo = VeloxKryoRegistrar.getKryo()
  //   val kryo = KryoThreadLocal.kryoTL.get
  //   val msg = kryo.deserialize(bytes)
  //   //VeloxKryoRegistrar.returnKryo(kryo)
  //   (msg, requestId, isRequest)
  // }

  // def cast[A : Manifest](value: Any): Option[A] = {
  //   val erasure = manifest[A] match {
  //     case Manifest.Byte => classOf[java.lang.Byte]
  //     case Manifest.Short => classOf[java.lang.Short]
  //     case Manifest.Char => classOf[java.lang.Character]
  //     case Manifest.Long => classOf[java.lang.Long]
  //     case Manifest.Float => classOf[java.lang.Float]
  //     case Manifest.Double => classOf[java.lang.Double]
  //     case Manifest.Boolean => classOf[java.lang.Boolean]
  //     case Manifest.Int => classOf[java.lang.Integer]
  //     case m => m.erasure
  //   }
  //   if(erasure.isInstance(value)) Some(value.asInstanceOf[A]) else None
  // }


}


