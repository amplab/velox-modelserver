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

@Path("/add-rating/{user}")
@Consumes(MediaType.APPLICATION_JSON) 
// @Produces(MediaType.APPLICATION_JSON) 
public class AddRatingResource {


    private RatingsMatrix ratings;
    private ModelStorage model;
    private static final double lambda = 0.2L;

    public AddRatingResource(ModelStorage model, RatingsMatrix ratings) {
        this.model = model;
        this.ratings = ratings;
    }

    @POST
    public Boolean rateMovie(@PathParam("user") LongParam userId,
                             @Valid MovieRating rating) {
        ratings.addRating(rating, userId.get().longValue());



    }

    public double[] updateUserModel(user) {


        List<double[]> ratedMovieFactors = ratings.g



    }




















    private final ClientStore users;
    private final ClientStore items;
    private static final Logger LOGGER = LoggerFactory.getLogger(AddRatingResource.class);

    // TODO eventually we will want a shared cache among resources
    /* private final ConcurrentHashMap<Long, double[]> itemCache; */
    /* private final ConcurrentHashMap<Long, double[]> userCache; */

    public AddRatingResource(ClientStore users, ClientStore items) {
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
