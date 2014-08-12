package edu.berkeley.veloxms.writer;


import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.SerializationUtils;
import tachyon.TachyonURI;
import tachyon.r.sorted.ClientStore;
import tachyon.r.sorted.Utils;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Random;
import java.util.TreeMap;
import java.io.*;
import java.nio.ByteBuffer;
import java.util.Comparator;


public class WriteModel {

    private static String itemModelLoc = "tachyon://localhost:19998/item-model";
    private static String userModelLoc = "tachyon://localhost:19998/user-model";
    private static String ratingsLoc = "tachyon://localhost:19998/movie-ratings";
    private static String testLoc = "tachyon://localhost:19998/test-loc";
    private static int numFeatures = 50;

    // public WriteModel() { 
    //     System.out.println("Creating new model."); 
    //
    // } 

    public static byte[] long2ByteArr(long id) {
        ByteBuffer key = ByteBuffer.allocate(8);
        key.putLong(id);
        return key.array();
    }

    public static double[] parseFactors(String[] splits) {
        double[] factors = new double[splits.length - 1];
        for (int i = 1; i < splits.length; ++i) {
            factors[i - 1] = Double.parseDouble(splits[i]);
        }
        return factors;
    }

    public static double[] randomArray(int length) {
        Random rand = new Random();
        double[] arr = new double[length];
        for (int i = 0; i < length; ++i) {
            arr[i] = rand.nextDouble();
        }
        return arr;
    }

    public static void writeModelsFromFile(String[] args) throws Exception {
        String userModelFile = args[0];
        System.out.println("user file: " + userModelFile);
        String itemModelFile = args[1];
        System.out.println("item file: " + itemModelFile);
        String ratingsFile = args[2];
        System.out.println("ratings file: " + ratingsFile);
        // Read user model
        writeTreeMapToTachyon(readModel(userModelFile), userModelLoc);
        // Read item model
        writeTreeMapToTachyon(readModel(itemModelFile), itemModelLoc);
        // read ratings
        writeTreeMapToTachyon(readRatings(ratingsFile), ratingsLoc);
    }

    public static void writeTreeMapToTachyon(TreeMap<byte[], byte[]> data, String tachyonLoc) throws Exception {
        int partition = 0;
        ClientStore store = ClientStore.createStore(new TachyonURI(tachyonLoc));
        store.createPartition(partition);
        for (byte[] k : data.keySet()) {
            store.put(partition, k, data.get(k));
        }
        store.closePartition(partition);
    }

    private static class ByteComparator implements Comparator<byte[]> {
        public int compare(byte[] a, byte[] b) {
            return Utils.compare(a, b);
        }
    }

    public static TreeMap<byte[], byte[]> readModel(String modelFile) {
        // TreeMap<byte[], byte[]> model = new TreeMap<byte[], byte[]>(new Comparator<byte[]> () { 
        //     public int compare(byte[] a, byte[] b) { 
        //         return Utils.compare(a, b); 
        //     } 
        // }); 
        TreeMap<byte[], byte[]> model = new TreeMap<byte[], byte[]>(new ByteComparator());
        FileInputStream fis = null;
        InputStreamReader isr = null;
        BufferedReader br = null;
        try {
            fis = new FileInputStream(new File(modelFile));
            isr = new InputStreamReader(fis);
            br = new BufferedReader(isr);
            boolean done = false;
            while (!done) {
                String line = br.readLine();
                if (line == null) {
                    done = true;
                } else {
                    String[] splits = line.split(",");
                    long key = Long.parseLong(splits[0]);
                    double[] factors = parseFactors(splits);
                    model.put(long2ByteArr(key), SerializationUtils.serialize(factors));
                }
            }
        } catch (Exception e) {
            System.out.println("Error: " + e.getMessage());
            e.printStackTrace();
        } finally {
            IOUtils.closeQuietly(br);
            IOUtils.closeQuietly(isr);
            IOUtils.closeQuietly(fis);
        }
        return model;
    }

    // public TreeMap<Long, HashMap<Long, Integer>> readRatings(String ratingsFile) { 
    public static TreeMap<byte[], byte[]> readRatings(String ratingsFile) {
        // TreeMap<byte[], byte[]> ratings = new TreeMap<byte[], byte[]>(new Comparator<byte[]> () { 
        //     public int compare(byte[] a, byte[] b) { 
        //         return Utils.compare(a, b); 
        //     } 
        // }); 
        TreeMap<byte[], byte[]> ratings = new TreeMap<byte[], byte[]>(new ByteComparator());

        FileInputStream fis = null;
        InputStreamReader isr = null;
        BufferedReader br = null;
        try {
            fis = new FileInputStream(new File(ratingsFile));
            isr = new InputStreamReader(fis);
            br = new BufferedReader(isr);
            boolean done = false;
            long currentUser = -1L;
            HashMap<Long, Integer> currentUserRatings = null;
            while (!done) {
                String line = br.readLine();
                if (line == null) {
                    done = true;
                } else {
                    String[] splits = line.split("\\s+");
                    long userId = Long.parseLong(splits[0]);
                    long itemId = Long.parseLong(splits[1]);
                    int rating = Integer.parseInt(splits[2]);
                    // ignore timestamp for now
                    long timestamp = Long.parseLong(splits[3]);
                    // assume input file sorted by user id
                    if (userId > currentUser) {
                        if (currentUserRatings != null) {
                            ratings.put(long2ByteArr(currentUser), SerializationUtils.serialize(currentUserRatings));
                        }
                        currentUser = userId;
                        currentUserRatings = new HashMap<Long, Integer>();
                    }
                    currentUserRatings.put(itemId, rating);
                }
            }
            // Last entry won't get added, so add after done reading file
            if (currentUserRatings != null) {
                ratings.put(long2ByteArr(currentUser), SerializationUtils.serialize(currentUserRatings));
            }
        } catch (Exception e) {
            System.out.println("Error: " + e.getMessage());
            e.printStackTrace();
        } finally {
            IOUtils.closeQuietly(br);
            IOUtils.closeQuietly(isr);
            IOUtils.closeQuietly(fis);
        }
        return ratings;
    }

    public static void writeRandomModels(String[] args) {
        int partition = Integer.parseInt(args[0]);
        boolean create = Boolean.parseBoolean(args[1]);
        ClientStore items = null;
        try {

            if (create) {
                items = ClientStore.createStore(new TachyonURI(itemModelLoc));
            } else {
                items = ClientStore.getStore(new TachyonURI(itemModelLoc));
            }
        } catch (Exception e) {
            System.out.println("Exception getting item store: " + e.getMessage());
        }
        try {
            items.createPartition(partition);
        } catch (Exception e) {
            System.out.println("Exception creating item partition: " + e.getMessage());
        }
        try {
            items.put(partition,
                    long2ByteArr(1L),
                    SerializationUtils.serialize(randomArray(numFeatures)));


            items.put(partition,
                    long2ByteArr(2L),
                    SerializationUtils.serialize(randomArray(numFeatures)));

            items.put(partition,
                    long2ByteArr(3L),
                    SerializationUtils.serialize(randomArray(numFeatures)));
            items.closePartition(partition);
        } catch (Exception e) {
            System.out.println("Exception putting items " + e.getMessage());
        }

        try {

            ClientStore users;
            if (create) {
                users = ClientStore.createStore(new TachyonURI(userModelLoc));
            } else {
                users = ClientStore.getStore(new TachyonURI(userModelLoc));
            }
            users.createPartition(partition);
            users.put(partition,
                    long2ByteArr(7000L),
                    SerializationUtils.serialize(randomArray(numFeatures)));


            users.put(partition,
                    long2ByteArr(8000L),
                    SerializationUtils.serialize(randomArray(numFeatures)));

            users.put(partition,
                    long2ByteArr(9000L),
                    SerializationUtils.serialize(randomArray(numFeatures)));
            users.closePartition(partition);
        } catch (Exception e) {
            System.out.println("Exception with users: " + e.getMessage());
        }

    }

    public static void testByteArrCompare() {
        System.out.println("Testing byte arrays");

        for (long i = 2L; i <= 10000L; ++i) {
            for (long j = 1L; j < i; ++j) {
                int result = Utils.compare(ByteBuffer.wrap(long2ByteArr(j)), ByteBuffer.wrap(long2ByteArr(i)));
                // System.out.println(result); 
                if (result >= 0) {
                    System.out.println("uh oh, i: " + i + ", j: " + j);
                }
            }
        }
    }

    public static void testLookup(long start, long end) throws Exception{
        // System.out.println("Writing to test lookups."); 
        //
        // ClientStore writeRatings = null; 
        // try { 
        //     writeRatings = ClientStore.createStore(new TachyonURI(testLoc)); 
        //     writeRatings.createPartition(0); 
        //     for (long i = start; i < end; ++i) { 
        //         writeRatings.put(0, long2ByteArr(i), long2ByteArr(i*100)); 
        //     } 
        //     writeRatings.closePartition(0); 
        //     Thread.sleep(10); 
        // } catch (Exception e) { 
        //     // do nothing 
        //
        // } 
        //
        //
        //
        // System.out.println("Testing lookups"); 
        //
        // ClientStore ratings = null; 
        // try { 
        //     ratings = ClientStore.getStore(new TachyonURI(testLoc)); 
        // } catch (Exception e) { 
        //     System.out.println("Couldn't find store"); 
        //     return; 
        // } 
        // byte[] rawBytes = null; 
        // for (long i = start; i < end; ++i) { 
        //         rawBytes = ratings.get(long2ByteArr(i)); 
        //     if (rawBytes != null) { 
        //         System.out.println("Successfully found: " + i); 
        //     } else { 
        //         System.out.println("Uh oh. Should have found: " + i); 
        //     } 
        // } 
    }


    public static void main(String[] args) throws Exception {
        String command = args[1];
        String[] droppedArgs = Arrays.copyOfRange(args, 2, args.length);
        if (command.equals("randomModel")) {
            // drop first 2 elements of args (program name, operation)
            writeRandomModels(droppedArgs);
        } else if (command.equals("writeModels")) {
            System.out.println("Writing models from file.");
            writeModelsFromFile(droppedArgs);
        // } else if (command.equals("testcompare")) { 
        //     testByteArrCompare(); 
        // } else if (command.equals("testlookups")) { 
        //     testLookup(Long.parseLong(droppedArgs[0]), Long.parseLong(droppedArgs[1])); 
        } else {
            // byte[] test = long2ByteArr(135L); 
            // for (int i = 0; i < test.length; ++i) { 
            //     System.out.println(test[i]); 
            // } 
            System.out.println(args[1] + " is not a valid command.");

        }
    }

}
