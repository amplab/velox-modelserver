package edu.berkeley.veloxms;

import io.dropwizard.Application;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import edu.berkeley.veloxms.resources.PredictItemResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.client.kv.KVStore;

public class VeloxApplication extends Application<VeloxConfiguration> {

    private static final Logger LOGGER = LoggerFactory.getLogger(VeloxApplication.class);

    public static void main(String[] args) throws Exception {
        new VeloxApplication().run(args);
    }

    @Override
    public String getName() {
        return "hello-world";
    }

    @Override
    public void initialize(Bootstrap<VeloxConfiguration> bootstrap) {
        // TODO read global model parameters?
        // TODO init tachyon client?
    }

    @Override
    public void run(VeloxConfiguration configuration,
            Environment environment) {
        FakeTachyonClient kvstore = new FakeTachyonClient();
        try {
            KVStore tachyonclient = KVStore.get("/veloxms/test-model");
        } catch (Exception e) {

            LOGGER.info("Caught Tachyon exception: " + e.getMessage());
        }
        final PredictItemResource userPredictor = new PredictItemResource(kvstore);
        environment.jersey().register(userPredictor);

    }


}
