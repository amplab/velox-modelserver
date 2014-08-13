package edu.berkeley.veloxms.resources;

import edu.berkeley.veloxms.storage.ModelStorage;
import io.dropwizard.jersey.params.LongParam;
import com.codahale.metrics.annotation.Timed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

@Path("/predict-item-materialized/{item}/{user}")
@Produces(MediaType.APPLICATION_JSON)
public class PredictFromMaterializedResource {

    private static final Logger LOGGER = LoggerFactory.getLogger(PredictItemResource.class);
    private final ModelStorage model;

    public PredictFromMaterializedResource(ModelStorage model) {
        this.model = model;
    }

    @GET
    @Timed
    public double getPrediction(@PathParam("user") LongParam userId,
            @PathParam("item") LongParam itemId) {
        return model.getMaterializedPrediction(userId.get().longValue(),
                                               itemId.get().longValue());
    }
}
