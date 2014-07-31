package edu.berkeley.veloxms;

import java.util.concurrent.ConcurrentHashMap;

public class FakeTachyonClient {
    
    private final ConcurrentHashMap<Long, double[]> kvstore;

    public FakeTachyonClient() {
        this.kvstore = new ConcurrentHashMap<Long, double[]>(64);
        this.kvstore.put(1L, doubleArray(1.0, 1.5, 3.5, 0.6));
        this.kvstore.put(2L, doubleArray(9.1, 2.5, 7.879, 0.2));
    }

    public double[] get(long key) {
        return this.kvstore.get(key);
    }

    public void put(long key, double[] value) {
        this.kvstore.put(key, value);
    }

    public static double[] doubleArray(double... elems) {
        return elems;
    }
}
