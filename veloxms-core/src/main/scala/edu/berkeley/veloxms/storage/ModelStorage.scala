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
 * @tparam T The type of the stored keys
 * @tparam U The type of the stored values
 */
trait ModelStorage[T, U] extends Managed {

    override def start() { }

    def put(kv: (T, U)): Unit

    def get(key: T): Option[U]
}


