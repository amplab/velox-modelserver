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

@Path("/predict-item-materialized/{user}")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class PredictFromMaterializedResource {

    private static final Logger LOGGER = LoggerFactory.getLogger(PredictItemResource.class);
    private final ModelStorage model;

    public PredictFromMaterializedResource(ModelStorage model) {
        this.model = model;
    }

    @GET
    @Timed
    public double getPrediction(@PathParam("user") LongParam userId,
            @QueryParam("item") LongParam itemId) {
        return model.getMaterializedPrediction(userId.get().longValue(),
                                               itemId.get().longValue());
    }

    @POST
    @Timed
    public long predictionFromBaseSet(@PathParam("user") LongParam userId,
            @Valid BaseItemSet baseItemSet) {

        double bestPrediction = -Double.MAX_VALUE;
        long bestItem = -1L;
        long uid = userId.get().longValue();

        for (Long item: baseItemSet.getItems()) {
            double prediction = model.getMaterializedPrediction(uid, item.longValue());
            if (prediction > bestPrediction) {
                bestPrediction = prediction;
                bestItem = item;
            }
        }
        return bestItem;
    }
}
