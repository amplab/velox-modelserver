package edu.berkeley.veloxms.storage;


import java.util.*;
import org.apache.commons.lang3.SerializationUtils;
import java.nio.ByteBuffer;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.r.sorted.ClientStore;

public class TachyonStorage implements ModelStorage {

    private final ClientStore users;
    private final ClientStore items;

    // TODO eventually we will want a shared cache among resources
    /* private final ConcurrentHashMap<Long, double[]> itemCache; */
    /* private final ConcurrentHashMap<Long, double[]> userCache; */

    public TachyonStorage(ClientStore users, ClientStore items) {
        this.users = users;
        this.items = items;
    }

    public double[] getItemFactors(long itemId) {
        return getFeatures(itemId, items);
    }

    public double[] getUserFactors(long userId) {
        return getFeatures(userId, users);
    }

    public static double[] getFactors(long id, ClientStore model) {
        ByteBuffer key = ByteBuffer.allocate(8);
        key.putLong(id);
        byte[] rawBytes = null;
        try {
            rawBytes = model.get(key.array());
            return (double[]) SerializationUtils.deserialize(rawBytes);
        } catch (IOException e) {
            LOGGER.warn("Caught tachyon exception: " + e.getMessage());
        }
        return null;
    }

    // public boolean updateUser 

}
