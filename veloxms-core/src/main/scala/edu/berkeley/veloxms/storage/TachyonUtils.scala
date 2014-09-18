package edu.berkeley.veloxms.storage

import java.nio.ByteBuffer
import java.io.IOException
import scala.util.{Try, Success, Failure}


/**
 * This class contains some static methods to make interacting with Tachyon
 * easier.
 */
object TachyonUtils {

    def long2ByteArr(id: Long): Array[byte] = {
        val key = ByteBuffer.allocate(8)
        key.putLong(id).array()

    }

    // could make this a z-curve key instead
    def twoDimensionKey(key1: Long, key2: Long): Array[byte] = {
        val key = ByteBuffer.allocate(16)
        key.putLong(key1)
        key.putLong(key2)
        key.array()
    }

    def getStore(url: String): Try[ClientStore] = {
        Try(ClientStore.getStore(new TachyonURI(url)))
    }


}
