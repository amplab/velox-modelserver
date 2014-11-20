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
  val table: String = ""
  val path: String = ""
  val totalNumUsers: Int = -1
  val numItems: Int = -1
  val numPartitions: Int = -1
  val modelSize: Int = -1
  val partition: Int = -1

  def build[T, U](env: Environment): ModelStorage[T, U] = {
    // Build the modelStorage
    val modelStorage: ModelStorage[T, U] = storageType match {
      case "local" => {
        logInfo("Using local storage")
        JVMLocalStorage[T, U](path)
      }
      case "jvmRandomUsers" => {
        logInfo(s"Using jvmRandom storage as partition $partition")
        JVMLocalStorage.generateRandomUserData(
            totalNumUsers,
            numPartitions,
            partition,
            modelSize).asInstanceOf[ModelStorage[T, U]]
      }
      case "jvmRandomItems" => {
        logInfo(s"Using jvmRandom storage as partition $partition")
        JVMLocalStorage.generateRandomItemData(
          numItems,
          modelSize).asInstanceOf[ModelStorage[T, U]]
      }
      case "jvmRandomObservations" => {
        logInfo(s"Using jvmRandom storage as partition $partition")
        JVMLocalStorage.generateRandomObservationData(
          totalNumUsers,
          numItems,
          numPartitions,
          partition,
          modelSize).asInstanceOf[ModelStorage[T, U]]
      }

      case "tachyon" => {
        logInfo("Using tachyon storage")
        env.jersey().register(new WriteModelsResource)
        new TachyonStorage[T, U](
          address,
          path)
      }
      case "cassandra" => {
        logInfo("Using cassandra storage")
        new CassandraStorage[T, U](
          address,
          keyspace,
          table)
      }
      case "rocks" => {
        logInfo("Using RocksDB storage")
        new RocksStorage[T, U](path)
      }
    }

    // Manage the modelStorage
    env.lifecycle().manage(modelStorage)

    // Return the modelStorage
    modelStorage
  }
}
