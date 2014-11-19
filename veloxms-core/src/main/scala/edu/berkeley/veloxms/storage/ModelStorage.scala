package edu.berkeley.veloxms.storage

import io.dropwizard.lifecycle.Managed
import org.codehaus.jackson.JsonNode

import scala.util.Try
import edu.berkeley.veloxms._

// import scala.collection.immutable.HashMap

/**
 * Simple interface to abstract out the KV storage backend used to store
 * the models from the application logic to access them.
 *
 * @tparam K The type of the stored keys
 * @tparam V The type of the stored values
 */
trait ModelStorage[K, V] extends Managed {

    override def start() { }

    def put(kv: (K, V)): Unit

    def get(key: K): Option[V]
}


