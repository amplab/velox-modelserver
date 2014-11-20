package edu.berkeley.veloxms.storage

import org.rocksdb.{RocksDBException, RocksDB, Options}
import edu.berkeley.veloxms._
import edu.berkeley.veloxms.util.{Logging, KryoThreadLocal}
import scala.util._
import java.nio.ByteBuffer
import scala.collection.immutable.HashMap

class RocksStorage[K, V] ( path: String ) extends ModelStorage[K, V] with Logging {

  // this is a static method that loads the RocksDB C++ library
  RocksDB.loadLibrary()

  val database = RocksUtil.getOrCreateDb(path) match {
    case Success(s) => s
    case Failure(f) => throw new RuntimeException(
      s"Couldn't open database: ${f.getMessage}")
  }

  def stop() = {
    database.close()
  }

  override def put(kv: (K, V)): Unit = {
    val kryo = KryoThreadLocal.kryoTL.get
    val keyBytes = StorageUtils.toByteArr(kv._1)
    val valueBytes = StorageUtils.toByteArr(kv._2)
    database.put(keyBytes, valueBytes)
  }

  override def get(key: K): Option[V] = {
    try {
      val rawBytes = ByteBuffer.wrap(database.get(StorageUtils.toByteArr(key)))
      val kryo = KryoThreadLocal.kryoTL.get
      val value = kryo.deserialize(rawBytes).asInstanceOf[V]
      Some(value)
    } catch {
      case u: Throwable => None
    }
  }
}

object RocksUtil {

  def getOrCreateDb(path: String): Try[RocksDB] = {
    val options = new Options().setCreateIfMissing(true)
    try {
      Success(RocksDB.open(options, path))
    } catch {
      case ex: RocksDBException => {
        Failure(new RuntimeException(ex.getMessage()))
      }
    }
  }
}