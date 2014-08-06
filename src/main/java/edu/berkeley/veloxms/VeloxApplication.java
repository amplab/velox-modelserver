package edu.berkeley.veloxms;

import io.dropwizard.Application;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import edu.berkeley.veloxms.resources.PredictItemResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.r.sorted.ClientStore;
import tachyon.TachyonURI;

public class VeloxApplication extends Application<VeloxConfiguration> {

    private static final Logger LOGGER = LoggerFactory.getLogger(VeloxApplication.class);

    public static void main(String[] args) throws Exception {
        new VeloxApplication().run(args);
    }

    @Override
    public String getName() {
        return "velox-modelserver, serving models since 2014";
    }

    @Override
    public void initialize(Bootstrap<VeloxConfiguration> bootstrap) {
        // TODO read global model parameters?
        // TODO init tachyon client?
    }

    @Override
    public void run(VeloxConfiguration config,
            Environment environment) {
        ClientStore userModel = null;
        ClientStore itemModel = null;
        ClientStore ratings = null;
        try {
            userModel = ClientStore.getStore(new TachyonURI(config.getUserModelLoc()));
            itemModel = ClientStore.getStore(new TachyonURI(config.getItemModelLoc()));
            ratings = ClientStore.getStore(new TachyonURI(config.getRatingsLoc()));
        } catch (Exception e) {

            LOGGER.error("Caught Tachyon exception: " + e.getMessage());
        }
        if (userModel == null || itemModel == null) {

            throw new RuntimeException("couldn't initialize models");
        }


        TachyonClientManager tachyonClientManager =
            new TachyonClientManager(userModel, itemModel, ratings, config.getNumFactors());
        environment.lifecycle().manage(tachyonClientManager);

        final PredictItemResource userPredictor =
            new PredictItemResource(new TachyonStorage(userModel, itemModel));
        environment.jersey().register(userPredictor);
        final AddRatingResource addRatings = new AddRatingResource();
        environment.jersey().register(addRatings);
    }
}






