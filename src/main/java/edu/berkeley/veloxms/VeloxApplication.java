package edu.berkeley.veloxms;

import edu.berkeley.veloxms.resources.AddRatingResource;
import edu.berkeley.veloxms.resources.PredictItemResource;
import edu.berkeley.veloxms.resources.PredictFromMaterializedResource;
import edu.berkeley.veloxms.storage.ModelStorage;
import edu.berkeley.veloxms.storage.TachyonStorage;
import io.dropwizard.Application;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tachyon.TachyonURI;
import tachyon.r.sorted.ClientStore;

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
        ClientStore matPredictions = null;
        try {
            userModel = ClientStore.getStore(new TachyonURI(config.getUserModelLoc()));
            itemModel = ClientStore.getStore(new TachyonURI(config.getItemModelLoc()));
            ratings = ClientStore.getStore(new TachyonURI(config.getRatingsLoc()));
            matPredictions = ClientStore.getStore(new TachyonURI(config.getMatPredictionsLoc()));
        } catch (Exception e) {

            LOGGER.error("Caught Tachyon exception: " + e.getMessage());
        }
        if (userModel == null || itemModel == null) {

            throw new RuntimeException("couldn't initialize models");
        }


        // TachyonClientManager tachyonClientManager = 
        //     new TachyonClientManager(userModel, itemModel);
        // environment.lifecycle().manage(tachyonClientManager); 

        ModelStorage model = new TachyonStorage(userModel,
                                                  itemModel,
                                                  ratings,
                                                  matPredictions,
                                                  config.getNumFactors());

        final PredictItemResource userPredictor =
            new PredictItemResource(model);
        environment.jersey().register(userPredictor);
        final PredictFromMaterializedResource matPredictor =
            new PredictFromMaterializedResource(model);
        environment.jersey().register(matPredictor);
        final AddRatingResource addRatings = new AddRatingResource(model);
        environment.jersey().register(addRatings);
    }
}







