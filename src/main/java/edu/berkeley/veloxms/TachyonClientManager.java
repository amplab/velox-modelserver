package edu.berkeley.veloxms;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tachyon.r.sorted.ClientStore;

import io.dropwizard.lifecycle.Managed;

public class TachyonClientManager implements Managed {

    private final ClientStore users;
    private final ClientStore items;

    private static final Logger LOGGER = LoggerFactory.getLogger(TachyonClientManager.class);

    public TachyonClientManager(ClientStore users, ClientStore items) {
        this.users = users;
        this.items = items;

    }

    @Override
    public void start() throws Exception {

        LOGGER.info("Starting tachyon clients (NO-OP)");
    }

    @Override
    public void stop() throws Exception {

        // TODO: Stopping tachyon clients. Should probably flush write buffers
        // and close partitions here eventually.
        
    }

}
