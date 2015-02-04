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
  // val modelSize: Int = -1
  val partition: Int = -1
  val docLength: Int = -1
  val ngramFile: String = ""
  val percentOfItems: Double = -1.0

  def build[T, U](env: Environment, modelSize: Int = 50, partition: Int = 0): ModelStorage[T, U] = {
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
          10.0,
          percentOfItems).asInstanceOf[ModelStorage[T, U]]
      }

      case "jvmEmptyObservationData" => {
        logInfo(s"Using jvm storage as partition $partition")
        JVMLocalStorage.generateEmptyObservationData(
          totalNumUsers,
          numPartitions,
          partition).asInstanceOf[ModelStorage[T, U]]
      }


      case "jvmRandomDocs" => {
        logInfo(s"Using jvm storage as partition $partition")
        JVMLocalStorage.generateRandomDocObservations(
          totalNumUsers,
          numItems,
          numPartitions,
          partition,
          docLength,
          ngramFile,
          10.0,
          percentOfItems).asInstanceOf[ModelStorage[T, U]]
      }

      case "tachyon" => {
        logInfo("Using tachyon storage")
        env.jersey().register(new WriteModelsResource)
        new TachyonStorage[T, U](
          address,
          path)
      }
      // case "tachyonRandom" => {
      //   logInfo("Using tachyon storage")
      //   env.jersey().register(new WriteModelsResource)
      //   TachyonStorage.writeRandomModels(
      //     numPartitions,
      //     totalNumUsers,
      //     numItems,
      //     users,
      //     items,
      //     ratings,
      //     numFactors)
      //   val sleepTime = 1000*30
      //   Thread.sleep(sleepTime)
      //   new TachyonStorage(
      //     address,
      //     users,
      //     items,
      //     ratings,
      //     numFactors)
      // }
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
