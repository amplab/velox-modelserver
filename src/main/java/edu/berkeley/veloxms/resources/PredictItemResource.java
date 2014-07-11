package edu.berkeley.veloxms.resources;

import edu.berkeley.veloxms.FakeTachyonClient;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.util.Random;
import io.dropwizard.jersey.params.LongParam;

import tachyon.client.kv.KVStore;

@Path("/predict-item/{user}")
@Produces(MediaType.APPLICATION_JSON)
public class PredictItemResource {
    private final FakeTachyonClient kvstore;
    private final double[] model = {0.1, 0.2, 0.3, 0.4};
    private final Random rand;

    public PredictItemResource(FakeTachyonClient kvstore) {
        this.kvstore = kvstore;
        rand = new Random();
    }

    @GET
    public int getPrediction(@PathParam("user") LongParam userId) {
        final double[] userFeatures = kvstore.get(userId.get());
        return makePrediction(userFeatures);
    }

    // make random predictions of x \in {0, 1}
    private int makePrediction(double[] userFeatures) {
        return rand.nextInt(2);
    }

}
