package edu.berkeley.veloxms.storage


import org.apache.commons.lang3.NotImplementedException
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import tachyon.r.sorted.ClientStore
import tachyon.TachyonURI
import scala.util._
import java.io.IOException
// import java.util.HashMap
import java.nio.ByteBuffer
import com.typesafe.scalalogging._
// import scala.collection.mutable
import scala.collection.immutable.HashMap
import edu.berkeley.veloxms._
import java.nio.ByteBuffer
import java.io.ByteArrayOutputStream
import edu.berkeley.veloxms.util.{VeloxKryoRegistrar, KryoThreadLocal}
// import scala.pickling._
// import binary._

// class TachyonStorage[U: ClassTag] (
class TachyonStorage (
    users: ClientStore,
    items: ClientStore,
    ratings: ClientStore,
    // val numFactors: Int) extends ModelStorage[U] with LazyLogging {
    val numFactors: Int) extends ModelStorage[FeatureVector] with LazyLogging {

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
          logger.info(s"FOUND raw bytes: $rawBytes")
          val kryo = KryoThreadLocal.kryoTL.get
          val array = kryo.deserialize(rawBytes).asInstanceOf[FeatureVector]
          logger.info(s"DESERIALIZED raw bytes: $array")
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
      val rawBytes = ByteBuffer.wrap(users.get(TachyonUtils.long2ByteArr(userId)))
      val kryo = KryoThreadLocal.kryoTL.get
      val result = kryo.deserialize(rawBytes).asInstanceOf[WeightVector]
      Success(result)


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
      val result = kryo.deserialize(rawBytes).asInstanceOf[HashMap[Long, Double]]
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

}



object TachyonUtils {

    def long2ByteArr(id: Long): Array[Byte] = {
        val key = ByteBuffer.allocate(8)
        key.putLong(id).array()
    }

    // could make this a z-curve key instead
    def twoDimensionKey(key1: Long, key2: Long): Array[Byte] = {
        val key = ByteBuffer.allocate(16)
        key.putLong(key1)
        key.putLong(key2)
        key.array()
    }

    def getStore(url: String): Try[ClientStore] = {
        Try(ClientStore.getStore(new TachyonURI(url)))
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



