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

class TachyonStorage[U] (
    users: ClientStore,
    items: ClientStore,
    ratings: ClientStore,
    val numFactors: Int) extends ModelStorage[U] with LazyLogging {

    def getFeatureData(itemId: Long): Try[U] = {
        // getFactors(itemId, items, "item-model")
        // for {
        //     rawBytes <- Try(items.get(TachyonUtils.long2ByteArr(userId)))
        //     array <- Try(SerializationUtils.deserialize(rawBytes))
        //     result <- Try(array match
        //         case Failure(u) => array
        //         case Success(u) => u.asInstanceOf[U]
        //     )
        // } yield result

        val result = for {
            rawBytes <- Try(items.get(TachyonUtils.long2ByteArr(itemId)))
            array <- Try(SerializationUtils.deserialize(rawBytes))
        } yield array
        result match {
            case Success(u) => Success(u.asInstanceOf[U])
            case Failure(u) => result
        }

    }

    def getUserFactors(userId: Long): Try[WeightVector] = {
        val result = for {
            rawBytes <- Try(users.get(TachyonUtils.long2ByteArr(userId)))
            array <- Try(SerializationUtils.deserialize(rawBytes))
        } yield array
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


}



