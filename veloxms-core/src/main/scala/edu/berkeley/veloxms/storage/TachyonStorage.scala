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
class TachyonStorage (
                         tachyonMaster: String,
                         userStoreName: String,
                         itemStoreName: String,
                         ratingsStoreName: String,
                         // val numFactors: Int) extends ModelStorage[U] with LazyLogging {
                         val numFactors: Int) extends ModelStorage[FeatureVector] with Logging {

  val users = TachyonUtils.getStore(tachyonMaster, userStoreName) match {
    case Success(s) => s
    case Failure(f) => throw new RuntimeException(
      s"Couldn't initialize use model: ${f.getMessage}")
  }

  val items = TachyonUtils.getStore(tachyonMaster, itemStoreName) match {
    case Success(s) => s
    case Failure(f) => throw new RuntimeException(
      s"Couldn't initialize item model: ${f.getMessage}")
  }

  val ratings = TachyonUtils.getStore(tachyonMaster, ratingsStoreName) match {
    case Success(s) => s
    case Failure(f) => throw new RuntimeException(
      s"Couldn't initialize use model: ${f.getMessage}")
  }

  logInfo("got tachyon stores")

  val usersCache = new ConcurrentHashMap[Long, WeightVector]

  val observationsCache = new ConcurrentHashMap[Long, Map[Long, Double]]

  def getFeatureData(itemId: Long): Try[FeatureVector] = {
    // getFactors(itemId, items, "item-model")
    // for {
    //     rawBytes <- Try(items.get(TachyonUtils.long2ByteArr(userId)))
    //     array <- Try(SerializationUtils.deserialize(rawBytes))
    //     result <- Try(array match
    //         case Failure(u) => array
    //         case Success(u) => u.asInstanceOf[U]
    //     )
    // } yield result

    try {
      val rawBytes = ByteBuffer.wrap(items.get(TachyonUtils.long2ByteArr(itemId)))
      val kryo = KryoThreadLocal.kryoTL.get
      val array = kryo.deserialize(rawBytes).asInstanceOf[FeatureVector]
      Success(array)
    } catch {
      case u: Throwable => Failure(u)
    }
    //
    // val (raw, result) = for {
    //     rawBytes <- Try(items.get(TachyonUtils.long2ByteArr(itemId)))
    //     array <- Try(SerializationUtils.deserialize(rawBytes))
    // } yield (rawBytes, array)
    // logger.info(s"Found item $raw")
    // result match {
    //     case Success(u) => Success(u.asInstanceOf[U])
    //     case Failure(u) => result
    // }
    //
  }

  def getUserFactors(userId: Long): Try[WeightVector] = {
    val mapResult = usersCache.get(userId)
    if (mapResult != null) {
      Success(mapResult)
    } else {
      val rawBytes = ByteBuffer.wrap(users.get(TachyonUtils.long2ByteArr(userId)))
      val kryo = KryoThreadLocal.kryoTL.get
      val result = kryo.deserialize(rawBytes).asInstanceOf[WeightVector]
      Success(result)
    }


    //
    // val result = for {
    //     rawBytes <- Try(users.get(TachyonUtils.long2ByteArr(userId)))
    //     array <- Try(rawBytes.unpickle[WeightVector])
    // } yield array
    // result
  }

  def getAllObservations(userId: Long): Try[Map[Long, Double]] = {

    val rawBytes = ByteBuffer.wrap(ratings.get(TachyonUtils.long2ByteArr(userId)))
    val kryo = KryoThreadLocal.kryoTL.get
    var result = kryo.deserialize(rawBytes).asInstanceOf[HashMap[Long, Double]]

    val cacheEntry = observationsCache.get(userId)
    if(cacheEntry != null) {
      for (iid <- cacheEntry.keys)  {
        if (result.keySet.contains(iid)) { // if there is an entry, then remove it
          result = result - iid
        }

        val cacheEntryVal: Double = cacheEntry.get(iid) match {
          case Some(cacheEntryVal) => cacheEntryVal
          case None => throw new RuntimeException("iid in key set not found on get")
        }

        result = result + (iid -> cacheEntryVal)
      }
    }

    Success(result)
      //
      // val result = for {
      //     rawBytes <- Try(ratings.get(TachyonUtils.long2ByteArr(userId)))
      //     array <- Try(rawBytes.unpickle[Map[Long, Double]])
      // } yield array
      // result
  }

  // def cast(value: Any): Option[A] = {
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

  def addObservation(userId: Long, itemId: Long, observation: Double) = {
    // eventually, this should write through to Tachyon
    var cacheEntry = observationsCache.get(userId)
    if(cacheEntry == null) {
      var newEntry = new HashMap[Long, Double]
      newEntry = newEntry + (itemId -> observation)
      observationsCache.put(userId, newEntry)
    } else {
      cacheEntry = cacheEntry + (itemId -> observation)
      observationsCache.remove(userId)
      observationsCache.put(userId, cacheEntry)
    }
  }

  /**
   * Cleans up any necessary resources
   */
  override def stop() {}
}



object TachyonUtils {

  def long2ByteArr(id: Long): Array[Byte] = {
    // val key = ByteBuffer.allocate(8)
    // key.putLong(id).array()

    val buffer = ByteBuffer.allocate(12)
    val kryo = KryoThreadLocal.kryoTL.get
    val result = kryo.serialize(id, buffer).array
    result
  }

  // could make this a z-curve key instead
  def twoDimensionKey(key1: Long, key2: Long): Array[Byte] = {
    val key = ByteBuffer.allocate(16)
    key.putLong(key1)
    key.putLong(key2)
    key.array()
  }

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


