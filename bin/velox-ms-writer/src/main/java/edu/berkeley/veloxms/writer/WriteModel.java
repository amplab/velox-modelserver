package edu.berkeley.veloxms.writer;


import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.SerializationUtils;
import tachyon.TachyonURI;
import tachyon.Pair;
import tachyon.r.sorted.ClientStore;
import tachyon.r.sorted.Utils;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.*;
import java.io.*;
import java.nio.ByteBuffer;
import java.util.Comparator;


public class WriteModel {

    private static String TACHYON_HOST = "ec2-54-87-239-99.compute-1.amazonaws.com";
    private static String itemModelLoc = "tachyon://" + TACHYON_HOST + ":19998/item-model";
    private static String userModelLoc = "tachyon://" + TACHYON_HOST + ":19998/user-model";
    private static String ratingsLoc = "tachyon://" + TACHYON_HOST + ":19998/movie-ratings";
    private static String testLoc = "tachyon://" + TACHYON_HOST + ":19998/test-loc";
    private static String matPredictionsLoc = "tachyon://" + TACHYON_HOST + ":19998/mat-predictions";
    private static int numFeatures = 50;

    public WriteModel() {
        System.out.println("Creating new model writer.");

    }

    public static byte[] long2ByteArr(long id) {
        ByteBuffer key = ByteBuffer.allocate(8);
        key.putLong(id);
        return key.array();
    }

    public static byte[] double2ByteArr(double d) {
        ByteBuffer key = ByteBuffer.allocate(8);
        key.putDouble(d);
        return key.array();
    }

    public static byte[] twoDimensionKey(long key1, long key2) {
        ByteBuffer key = ByteBuffer.allocate(16);
        key.putLong(key1);
        key.putLong(key2);
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
        writeTreeMapToTachyon(serializeModel(readModel(userModelFile)), userModelLoc);
        // Read item model
        writeTreeMapToTachyon(serializeModel(readModel(itemModelFile)), itemModelLoc);
        // read ratings
        writeTreeMapToTachyon(readRatings(ratingsFile), ratingsLoc);

    }

    private class WritePredictions implements Runnable {
        private final NavigableMap<byte[], Pair<Long, double[]>> userModel;
        private final NavigableMap<Long, double[]> itemModel;
        private TachyonURI writeLoc;
        private int part;
        private int threadNum;
        // 2*10^6
        public static final int PART_SIZE = 5*100000;

        public WritePredictions(NavigableMap<byte[], Pair<Long, double[]>> users, NavigableMap<Long, double[]> items, int partitionStart, int threadNum) {
            this.userModel = users;
            this.itemModel = items;
            this.writeLoc = new TachyonURI(matPredictionsLoc);
            this.part = partitionStart;
            this.threadNum = threadNum;
        }

        private double makePrediction(double[] userFeatures, double[] itemFeatures) {
            double sum = 0;
            for (int i = 0; i < userFeatures.length; ++i) {
                sum += itemFeatures[i]*userFeatures[i];
            }
            return sum;
        }

        public void run() {
            ClientStore store = null;
            try {
                store = ClientStore.getStore(writeLoc);
            } catch (IOException e) {
                System.out.println("Tachyon exception: " + e.getMessage());
                System.err.println("Returning early.");
                return;
            }
            TreeMap<byte[], byte[]> predictions = new TreeMap<byte[], byte[]>(new ByteComparator());
            int partition = this.part;
            int numPredictions = 0;
            for (byte[] userIdBytes: userModel.keySet()) {
                long userId = userModel.get(userIdBytes).getFirst(); 
                double[] userFeatures = userModel.get(userIdBytes).getSecond();
                for (Long itemId: itemModel.keySet()) {
                    byte[] key = WriteModel.twoDimensionKey(userId, itemId.longValue());
                    byte[] value = WriteModel.double2ByteArr(makePrediction(userFeatures,
                            itemModel.get(itemId)));
                    predictions.put(key, value);
                    numPredictions++;
                    if (numPredictions % PART_SIZE == 0) {
                        try {
                            store.createPartition(partition);
                            for (byte[] k: predictions.keySet()) {
                                store.put(partition, k, predictions.get(k));
                            }
                            // empty map
                            predictions.clear();
                            store.closePartition(partition);
                        } catch (IOException e) {
                            System.out.println("Tachyon exception: " + e.getMessage());
                        }
                        System.out.println("Thread: " + threadNum + " writing partition: " + partition);
                        partition += 1;
                    }
                }
            }

            try {
                store.createPartition(partition);
                for (byte[] k: predictions.keySet()) {
                    store.put(partition, k, predictions.get(k));
                }
                // empty map
                predictions.clear();
                store.closePartition(partition);
            } catch (IOException e) {
                System.out.println("Tachyon exception: " + e.getMessage());
            }
        }
    }

    public void materializeAllPredictions(String[] args) throws Exception {
        String userModelFile = args[0];
        System.out.println("user file: " + userModelFile);
        String itemModelFile = args[1];
        System.out.println("item file: " + itemModelFile);
        final TreeMap<Long, double[]> userModel = readModel(userModelFile);
        TreeMap<byte[], Pair<Long, double[]>> userModelSorted = new TreeMap<byte[], Pair<Long, double[]>>(new ByteComparator());
        for (Long k: userModel.keySet()) {
            userModelSorted.put(long2ByteArr(k.longValue()), new Pair<Long, double[]>(k, userModel.get(k)));
        }

        final TreeMap<Long, double[]> itemModel = readModel(itemModelFile);

        try {
            ClientStore store = ClientStore.createStore(new TachyonURI(matPredictionsLoc));
        } catch (IOException e) {
            System.out.println("Tachyon exception: " + e.getMessage());
            System.err.println("Returning early.");
            return;
        }

        int totalPredictions = userModel.size() * itemModel.size();
        int numThreads = 16;
        int userModelPartSize = userModel.size() / numThreads;
        int threadNum = 0;
        List<Thread> threads = new ArrayList<Thread>(16);
        byte[] startKey = userModelSorted.firstKey();
        int curPartCount = 0;
        int threadCountSpread = 100;
        for (byte[] k: userModelSorted.keySet()) {
            curPartCount += 1;
            if (curPartCount == userModelPartSize && threadNum < (numThreads - 1)) {
                threads.add(new Thread(new WritePredictions(
                                userModelSorted.subMap(startKey, true, k, false),
                                itemModel,
                                threadNum*threadCountSpread,
                                threadNum)));

                curPartCount = 0;
                startKey = k;
                threadNum += 1;
            }
        }

        threads.add(new Thread(new WritePredictions(
                        userModelSorted.subMap(startKey, true, userModelSorted.lastKey(), true),
                        itemModel,
                        threadNum*threadCountSpread,
                        threadNum)));

        System.out.println("Threads starting.");
        // start threads
        for (Thread t: threads) {
            t.start();
        }

        // wait for threads to finish
        int i = 0;
        for (Thread t: threads) {
            t.join();
            i += 1;
            System.out.println(i + " threads have completed.");
        }
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

    public static TreeMap<byte[], byte[]> serializeModel(TreeMap<Long, double[]> model) {
        TreeMap<byte[], byte[]> byteArrModel =
            new TreeMap<byte[], byte[]>(new ByteComparator());
        for (long key: model.keySet()) {
            byteArrModel.put(long2ByteArr(key), SerializationUtils.serialize(model.get(key)));
        }
        return byteArrModel;
    }

    public static TreeMap<Long, double[]> readModel(String modelFile) {
        TreeMap<Long, double[]> model = new TreeMap<Long, double[]>();
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
                    model.put(key, factors);
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

    public static TreeMap<byte[], byte[]> readRatings(String ratingsFile) {
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
            HashMap<Long, Float> currentUserRatings = null;
            while (!done) {
                String line = br.readLine();
                if (line == null) {
                    done = true;
                } else {
                    // String[] splits = line.split("\\s+"); 
                    String[] splits = line.split("::");
                    long userId = Long.parseLong(splits[0]);
                    long itemId = Long.parseLong(splits[1]);
                    float rating = Float.parseFloat(splits[2]);
                    // ignore timestamp for now
                    long timestamp = Long.parseLong(splits[3]);
                    // assume input file sorted by user id
                    if (userId > currentUser) {
                        if (currentUserRatings != null) {
                            ratings.put(long2ByteArr(currentUser), SerializationUtils.serialize(currentUserRatings));
                        }
                        currentUser = userId;
                        currentUserRatings = new HashMap<Long, Float>();
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

    // public static void testByteArrCompare() { 
    //     System.out.println("Testing byte arrays"); 
    //     for (long i = 2L; i <= 10000L; ++i) { 
    //         for (long j = 1L; j < i; ++j) { 
    //             int result = Utils.compare(ByteBuffer.wrap(long2ByteArr(j)), ByteBuffer.wrap(long2ByteArr(i))); 
    //             // System.out.println(result);  
    //             if (result >= 0) { 
    //                 System.out.println("uh oh, i: " + i + ", j: " + j); 
    //             } 
    //         } 
    //     } 
    // } 

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
        String command = args[0];
        String[] droppedArgs = Arrays.copyOfRange(args, 1, args.length);
        if (command.equals("randomModel")) {
            writeRandomModels(droppedArgs);
        } else if (command.equals("writeModels")) {
            System.out.println("Writing models from file.");
            writeModelsFromFile(droppedArgs);
        } else if (command.equals("matPredictions")) {
            System.out.println("Materializing all predictions.");
            WriteModel modelWriter = new WriteModel();
            modelWriter.materializeAllPredictions(droppedArgs);
        } else {
            System.out.println(args[0] + " is not a valid command.");
        }
    }

}
