package edu.berkeley.veloxms.storage;

import java.nio.ByteBuffer;
import java.io.IOException;


/**
 * This class contains some static methods to make interacting with Tachyon
 * easier.
 */
public class TachyonUtils {


    private TachyonUtils() {
        // private constructor, do nothing
    }

    public static byte[] long2ByteArr(long id) {
        ByteBuffer key = ByteBuffer.allocate(8);
        key.putLong(id);
        return key.array();
    }

}
