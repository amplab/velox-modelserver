package edu.berkeley.veloxms.storage

import java.nio.ByteBuffer
import edu.berkeley.veloxms.util.KryoThreadLocal

object StorageUtils {

  def toByteArr[T](id: T): Array[Byte] = {
    // val key = ByteBuffer.allocate(8)
    // key.putLong(id).array()

    //val buffer = ByteBuffer.allocate(12)
    val kryo = KryoThreadLocal.kryoTL.get
    val result = kryo.serialize(id).array
    result
  }

  // could make this a z-curve key instead
  def twoDimensionKey(key1: Long, key2: Long): Array[Byte] = {
    val key = ByteBuffer.allocate(16)
    key.putLong(key1)
    key.putLong(key2)
    key.array()
  }

}
