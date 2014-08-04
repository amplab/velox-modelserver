package edu.berkeley.veloxms.writer;


import tachyon.r.sorted.ClientStore;
import tachyon.TachyonURI;
import org.apache.commons.lang3.SerializationUtils;
import java.util.Random;
import java.nio.ByteBuffer;
import java.io.IOException;

public class WriteModel {

    private static String itemModelLoc = "tachyon://localhost:19998/item-model";
    private static String userModelLoc = "tachyon://localhost:19998/user-model";
    private static int numFeatures = 50;

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
        String itemModelFile = args[1];
        // Read user model
        HashMap<Long, double[]> userModel = readModel(userModelFile);
        writeHashMapToTachyon(userModelFile, userModelLoc);
        // Read item model
        HashMap<Long, double[]> itemModel = readModel(itemModelFile);
        writeHashMapToTachyon(itemModel, itemModelLoc);
    }

    public void writeHashMapToTachyon(HashMap<Long, double[]> model, String tachyonLoc) {
        try {
            ClientStore store = ClientStore.createStore(new TachyonURI(tachyonLoc));
            int partition = 0;
            store.createPartition(partition);
            for (Long k : model.keySet()) {
                store.put(partition,
                          long2ByteArr(k.longValue()),
                          SerializationUtils.serialize(model[k]));
            }

            store.closePartition(partition);
    }

    public HashMap<Long, double[]> readModel(String modelFile) {
        HashMap<Long, double[]> model = new HashMap<Long, double[]>();
        FileInputStream fis = null;
        InputStreamReader isr = null;
        BufferReader br = null;
        try {
            fis = new FileInputStream(modelFile);
            isr = new InputStreamReader(fis);
            br = new BufferReader(isr);
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
        } finally {
            IOUtils.closeQuietly(br);
            IOUtils.closeQuietly(isr);
            IOUtils.closeQuietly(fis);
        }
        return model;
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


    public static void main(String[] args) {
        WriteModel modelWrite = new WriteModel();
        String command = args[1];
        String[] droppedArgs = Array.copyOfRange(args, 2, args.length);
        if (args[1] == "randomModel") {
            // drop first 2 elements of args (program name, operation)
            modelWrite.writeRandomModels(droppedArgs);
        } else if (args[1] == "writeModels") {
            modelWrite.writeModelsFromFile(args);
        }
    }

}
