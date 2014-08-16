package edu.berkeley.veloxms.resources;

import edu.berkeley.veloxms.storage.ModelStorage;
import io.dropwizard.jersey.params.LongParam;
import com.codahale.metrics.annotation.Timed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import edu.berkeley.veloxms.BaseItemSet;

import javax.validation.Valid;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.Consumes;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.util.*;

@Path("/predict-item/{user}")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class PredictItemResource {

    private static final Logger LOGGER = LoggerFactory.getLogger(PredictItemResource.class);
    private final ModelStorage model;

    public PredictItemResource(ModelStorage model) {
        this.model = model;
    }

    @GET
    @Timed
    public double getPrediction(@PathParam("user") LongParam userId,
            @QueryParam("item") LongParam itemId) {
        double[] userFeatures = model.getUserFactors(userId.get().longValue());
        double[] itemFeatures = model.getItemFactors(itemId.get().longValue());
        return makePrediction(userFeatures, itemFeatures);
    }

    private double makePrediction(double[] userFeatures, double[] itemFeatures) {
        if (userFeatures == null || itemFeatures == null) {
            if (userFeatures == null) {
                LOGGER.warn("User features null. Returning 0 as prediction.");
            }
            if (itemFeatures == null) {
                LOGGER.warn("Item features null. Returning 0 as prediction.");
            }
            return 0;
        }
        double sum = 0;
        for (int i = 0; i < userFeatures.length; ++i) {
            sum += itemFeatures[i]*userFeatures[i];
        }
        return sum;
    }

    @POST
    @Timed
    public long predictionFromBaseSet(@PathParam("user") LongParam userId,
            @Valid BaseItemSet baseItemSet) {

        double[] userFeatures = model.getUserFactors(userId.get().longValue());
        double bestPrediction = -Double.MAX_VALUE;
        long bestItem = -1L;

        for (Long item: baseItemSet.getItems()) {
            double[] itemFeatures = model.getItemFactors(item.longValue());
            double prediction = makePrediction(userFeatures, itemFeatures);
            if (prediction > bestPrediction) {
                bestPrediction = prediction;
                bestItem = item;
            }
        }
        return bestItem;
    }
}
