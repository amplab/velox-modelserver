package edu.berkeley.veloxms.resources;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.util.Random;
import io.dropwizard.jersey.params.LongParam;
import java.util.*;
import org.apache.commons.lang3.SerializationUtils;
import java.nio.ByteBuffer;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.r.sorted.ClientStore;

@Path("/predict-item/{item}/{user}")
@Produces(MediaType.APPLICATION_JSON)
public class PredictItemResource {
    private final ClientStore users;
    private final ClientStore items;
    private static final Logger LOGGER = LoggerFactory.getLogger(PredictItemResource.class);

    // TODO eventually we will want a shared cache among resources
    /* private final ConcurrentHashMap<Long, double[]> itemCache; */
    /* private final ConcurrentHashMap<Long, double[]> userCache; */

    public PredictItemResource(ClientStore users, ClientStore items) {
        this.users = users;
        this.items = items;
    }

    @GET
    public double getPrediction(@PathParam("user") LongParam userId,
            @PathParam("item") LongParam itemId) {
        double[] userFeatures = getFeatures(userId.get().longValue(), users);
        double[] itemFeatures = getFeatures(itemId.get().longValue(), items);
        return makePrediction(userFeatures, itemFeatures);
    }


    public static double[] getFeatures(long id, ClientStore model) {
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


    private double makePrediction(double[] userFeatures, double[] itemFeatures) {
        double sum = 0;
        for (int i = 0; i < userFeatures.length; ++i) {
            sum += itemFeatures[i]*userFeatures[i];
        }
        return sum;
    }

}
