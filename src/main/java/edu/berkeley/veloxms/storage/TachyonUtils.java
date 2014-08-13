package edu.berkeley.veloxms.storage;

import java.nio.ByteBuffer;
import java.io.IOException;


/**
 * This class contains some static methods to make interacting with Tachyon
 * easier.
 */
public final class TachyonUtils {


    private TachyonUtils() {
        // private constructor, do nothing
    }

    public static byte[] long2ByteArr(long id) {
        ByteBuffer key = ByteBuffer.allocate(8);
        key.putLong(id);
        return key.array();
    }

    // could make this a z-curve key instead
    public static byte[] twoDimensionKey(long key1, long key2) {
        ByteBuffer key = ByteBuffer.allocate(16);
        key.putLong(key1);
        key.putLong(key2);
        return key.array();
    }



}
