package edu.berkeley.veloxms.resources;

import edu.berkeley.veloxms.MovieRating;
import edu.berkeley.veloxms.storage.ModelStorage;
import io.dropwizard.jersey.params.LongParam;
import com.codahale.metrics.annotation.Timed;
import org.jblas.DoubleMatrix;
import org.jblas.Solve;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.Valid;
import javax.ws.rs.Consumes;
import javax.ws.rs.Produces;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.MediaType;
import java.util.Arrays;
import java.util.HashMap;

@Path("/add-rating/{user}")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class AddRatingResource {


    private ModelStorage model;
    private static final double lambda = 0.2;
    private static final Logger LOGGER = LoggerFactory.getLogger(AddRatingResource.class);

    public AddRatingResource(ModelStorage model) {
        this.model = model;
    }

    @POST
    @Timed
    public Boolean rateMovie(@PathParam("user") LongParam userId,
                             @Valid MovieRating rating) {
        // TODO do something with the new user model
        long uid = userId.get().longValue();
        LOGGER.info("adding rating for user: " + uid);

        double[] newUserFactor = updateUserModel(uid, rating);
        LOGGER.info("old user factors: " + Arrays.toString(model.getUserFactors(uid)));
        LOGGER.info("new user factors: " + Arrays.toString(newUserFactor));
        return true;


    }

    // TODO write the new rating somewhere
    public double[] updateUserModel(long user, MovieRating newRating) {

        // List<double[]> ratedMovieFactors = ratings.g 
        HashMap<Long, Float> userMovieRatings = model.getRatedMovies(user);
        userMovieRatings.put(newRating.getMovieId(), newRating.getRating());


        // construct movie matrix:
        int k = model.getNumFactors();
        DoubleMatrix movieSum = DoubleMatrix.zeros(k, k);
        DoubleMatrix movieRatingsProductMatrix = DoubleMatrix.zeros(k);

        for(Long movieId: userMovieRatings.keySet()) {
            DoubleMatrix movieFactors = new DoubleMatrix(model.getItemFactors(movieId));
            DoubleMatrix result = movieFactors.mmul(movieFactors.transpose());
            // TODO make sure this addition is in place
            movieSum.addi(result);


            // compute mj*Rij
            movieRatingsProductMatrix.addi(movieFactors.muli(userMovieRatings.get(movieId)));
        }

        // add regularization term
        DoubleMatrix regularization = DoubleMatrix.eye(k);
        // FIXME Don't multiply by k here, multiply by number of non-zeros?
        regularization.muli(lambda*k);
        // throw new Exception("FIXME here."); 
        // TODO make sure this addition is an in-place operation
        movieSum.addi(regularization);


        // Compute matrix inverse by solving movieSum*X=I for X
        DoubleMatrix inverseMovieSum = Solve.solve(movieSum, DoubleMatrix.eye(k));

        DoubleMatrix newUserFactors = inverseMovieSum.mmul(movieRatingsProductMatrix);
        // TODO we can probably just keep everything as DoubleMatrix type, no need to convert
        // back and forth between matrices and arrays
        return newUserFactors.toArray();
    }
}
