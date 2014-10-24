package edu.berkeley.veloxms.storage

import edu.berkeley.veloxms.FeatureVector
import edu.berkeley.veloxms.misc.WriteModelsResource
import edu.berkeley.veloxms.util.Logging
import io.dropwizard.lifecycle.Managed
import io.dropwizard.setup.Environment
import org.hibernate.validator.constraints.NotEmpty

/**
 * A storage factory to provide the configured model storage
 */
class ModelStorageFactory extends Logging {
  @NotEmpty val storageType: String = ""

  val address: String = ""
  val keyspace: String = ""
  val items: String = ""
  val users: String = ""
  val ratings: String = ""

  def build(env: Environment, numFactors: Int): ModelStorage[FeatureVector] = {
    // Build the modelStorage
    val modelStorage: ModelStorage[FeatureVector] = storageType match {
      case "local" => {
        logInfo("Using local storage")
        JVMLocalStorage(
          users,
          items,
          ratings,
          numFactors)
      }
      case "tachyon" => {
        logInfo("Using tachyon storage")
        env.jersey().register(new WriteModelsResource)
        new TachyonStorage(
          address,
          users,
          items,
          ratings,
          numFactors)
      }
      case "cassandra" => {
        logInfo("Using cassandra storage")
        new CassandraStorage(
          address,
          keyspace,
          users,
          items,
          ratings,
          numFactors)
      }
    }

    // Manage the modelStorage
    env.lifecycle().manage(modelStorage)

    // Return the modelStorage
    modelStorage
  }
}
