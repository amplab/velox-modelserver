package edu.berkeley.veloxms.storage


import org.apache.commons.lang3.SerializationUtils
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
import scala.collection.mutable
import edu.berkeley.veloxms._

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
          val rawBytes = items.get(TachyonUtils.long2ByteArr(itemId))
          logger.info(s"FOUND raw bytes: $rawBytes")
          val array = SerializationUtils.deserialize(rawBytes)
          logger.info(s"DESERIALIZED raw bytes: $array")
          val result = array.asInstanceOf[FeatureVector]
          Success(result)
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
        val result = for {
            rawBytes <- Try(users.get(TachyonUtils.long2ByteArr(userId)))
            array <- Try(SerializationUtils.deserialize(rawBytes))
        } yield array
        logger.info(s"Found user $result")
        result match {
            case Success(u) => Success(u.asInstanceOf[WeightVector])
            case Failure(u) => result
        }
    }

    def getAllObservations(userId: Long): Try[Map[Long, Double]] = {


        val result = for {
            rawBytes <- Try(ratings.get(TachyonUtils.long2ByteArr(userId)))
            array <- Try(SerializationUtils.deserialize(rawBytes))
        } yield array
        result match {
            case Success(u) => Success(u.asInstanceOf[Map[Long, Double]])
            case Failure(u) => result
        }
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



