package edu.berkeley.veloxms.writer;


import tachyon.r.sorted.ClientStore;
import tachyon.TachyonURI;
import org.apache.commons.lang3.SerializationUtils;
import java.util.Random;
import java.util.Arrays;
import java.nio.ByteBuffer;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.FileInputStream;
import org.apache.commons.io.IOUtils;
import java.util.TreeMap;
import java.util.HashMap;
import java.io.File;
import tachyon.r.sorted.Utils;
import java.io.Serializable;

public class WriteModel {

    private static String itemModelLoc = "tachyon://localhost:19998/item-model";
    private static String userModelLoc = "tachyon://localhost:19998/user-model";
    private static String ratingsLoc = "tachyon://localhost:19998/movie-ratings";
    private static int numFeatures = 50;

    public WriteModel() {
        System.out.println("Creating new model.");

    }

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

    public void writeModelsFromFile(String[] args) {
        String userModelFile = args[0];
        System.out.println("user file: " + userModelFile);
        String itemModelFile = args[1];
        System.out.println("item file: " + itemModelFile);
        String ratingsFile = args[2];
        System.out.println("ratings file: " + ratingsFile);
        // Read user model
        TreeMap<Long, double[]> userModel = readModel(userModelFile);
        WriteModel.writeTreeMapToTachyon(userModel, userModelLoc);
        // Read item model
        TreeMap<Long, double[]> itemModel = readModel(itemModelFile);
        WriteModel.writeTreeMapToTachyon(itemModel, itemModelLoc);
        TreeMap<Long, HashMap<Long, Integer>> ratings = readRatings(ratingsFile);
        WriteModel.writeTreeMapToTachyon(ratings, ratingsLoc);
    }

    public static void writeTreeMapToTachyon(TreeMap<Long, ? extends Serializable> model, String tachyonLoc) {
        int partition = 0;
        ClientStore store = null;
        try {
            store = ClientStore.createStore(new TachyonURI(tachyonLoc));
            store.createPartition(partition);
            // Set<Long> keyset = model.keySet(); 

            for (Long k : model.keySet()) {
                // System.out.println(k); 
                store.put(partition,
                        long2ByteArr(k.longValue()),
                        SerializationUtils.serialize(model.get(k)));
            }
            store.closePartition(partition);
        } catch (Exception e) {
            System.out.println("Tachyon error: " + e.getMessage());
        }

    }

    public TreeMap<Long, double[]> readModel(String modelFile) {
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

    public TreeMap<Long, HashMap<Long, Integer>> readRatings(String ratingsFile) {
        TreeMap<Long, HashMap<Long, Integer>> ratings =
            new TreeMap<Long, HashMap<Long, Integer>>();

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
                        currentUser = userId;
                        currentUserRatings = new HashMap<Long, Integer>();
                        ratings.put(currentUser, currentUserRatings);
                    }
                    currentUserRatings.put(itemId, rating);
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
        return ratings;
    }

    public void writeRandomModels(String[] args) {
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

        for (long i = 1L; i <= 128L; ++i) {
            int result = Utils.compare(long2ByteArr(i), long2ByteArr(i + 1));
            // System.out.println(result); 
            if (result >= 0) {
                System.out.println("uh oh: " + i);
            }
        }
    }


    public static void main(String[] args) {
        WriteModel modelWrite = new WriteModel();
        String command = args[1];
        String[] droppedArgs = Arrays.copyOfRange(args, 2, args.length);
        if (command.equals("randomModel")) {
            // drop first 2 elements of args (program name, operation)
            modelWrite.writeRandomModels(droppedArgs);
        } else if (command.equals("writeModels")) {
            System.out.println("Writing models from file.");
            modelWrite.writeModelsFromFile(droppedArgs);
        } else if (command.equals("testcompare")) {
            testByteArrCompare();
        } else {
            // byte[] test = long2ByteArr(135L); 
            // for (int i = 0; i < test.length; ++i) { 
            //     System.out.println(test[i]); 
            // } 
            System.out.println(args[1] + " is not a valid command.");

        }
    }

}
