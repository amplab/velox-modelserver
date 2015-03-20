package edu.berkeley.veloxms.models

import javax.validation.constraints.NotNull

import com.fasterxml.jackson.annotation.JsonProperty
import edu.berkeley.veloxms._
import edu.berkeley.veloxms.storage.ModelStorageFactory
import edu.berkeley.veloxms.util.Logging
import io.dropwizard.setup.Environment
import org.hibernate.validator.constraints.NotEmpty
import scala.io.Source

/**
 * A storage factory to provide the configured model storage
 */
class ModelFactory extends Logging {
  @NotEmpty val modelType: String = ""
  @NotNull val modelSize: Integer = 50
  val partitionFile: String = ""
  val modelLoc: String = ""
  val cachePartialSums: Boolean = false
  val cachePredictions: Boolean = false
  val cacheFeatures: Boolean = false


  @NotNull
  @(JsonProperty)("storage")
  val modelStorageFactories: Map[String, ModelStorageFactory] = Map()

  def build(env: Environment, hostname: String): (Model[_, _], Int, Map[String, Int]) = {
    modelType match {
      case "MatrixFactorizationModel" => {
        require(modelStorageFactories.contains("users"))
        require(modelStorageFactories.contains("items"))
        require(modelStorageFactories.contains("ratings"))
        require(partitionFile != "")
        val (partition, partitionMap) = getPartition(partitionFile, hostname)
        require(partition > -1)
        

        // Build and manage the modelStorages
        val itemStore = modelStorageFactories
          .get("items").get
          .build[Long, FeatureVector](env, modelSize, partition)
        val userStore = modelStorageFactories
          .get("users").get
          .build[Long, WeightVector](env, modelSize, partition)
        val observationStore = modelStorageFactories
          .get("ratings").get
          .build[Long, Map[Long, Double]](env, modelSize, partition)
        val averageUser = Array.fill[Double](modelSize)(1.0)
        val model = new MatrixFactorizationModel(
            modelSize,
            itemStore,
            userStore,
            observationStore,
            averageUser,
            cachePartialSums,
            cacheFeatures,
            cachePredictions
          )
        val numUsers = modelStorageFactories.get("ratings").get.totalNumUsers
        val numPartitions = modelStorageFactories.get("ratings").get.numPartitions

        if (cachePartialSums) {
          model.precomputePartialSums(partition until numUsers by numPartitions)
        }
        (model, partition, partitionMap)

      }
      // To use the newsgroups model, the binary file containing the model must
      // be specified in modelLoc. The binary is not included in the repo, but can be
      // downloaded from https://s3.amazonaws.com/velox-public/news-classifier-from-tomer
      case "NewsgroupsModel" => {
        require(modelStorageFactories.contains("users"))
        require(modelStorageFactories.contains("ratings"))
        require(partitionFile != "")
        require(modelLoc != "")
        val (partition, partitionMap) = getPartition(partitionFile, hostname)
        require(partition > -1)

        // Build and manage the modelStorages
        val userStore = modelStorageFactories
          .get("users").get
          .build[Long, WeightVector](env, modelSize, partition)
        val observationStore = modelStorageFactories
          .get("ratings").get
          .build[Long, Map[String, Double]](env, modelSize, partition)
        val averageUser = Array.fill[Double](modelSize)(1.0)
        val model = new NewsgroupsModel(
            modelSize,
            userStore,
            observationStore,
            averageUser,
            modelLoc,
            cachePartialSums,
            cacheFeatures,
            cachePredictions
          )

        val numUsers = modelStorageFactories.get("ratings").get.totalNumUsers
        val numPartitions = modelStorageFactories.get("ratings").get.numPartitions

        if (cachePartialSums) {
          model.precomputePartialSums(partition until numUsers by numPartitions)
        }
        (model, partition, partitionMap)
      }
      case _ => {
        throw new IllegalArgumentException(s"Unknown model type: $modelType")
      }
    }
  }

  def getPartition(partitionFile: String, hostname: String): (Int, Map[String, Int]) = {
    logInfo(s"My host name: $hostname")
    val hosts = Source.fromFile(partitionFile)
      .getLines
      .map( line => {
        val splits = line.split(": ")
        val url = splits(0)
        val part = splits(1).toInt
        logInfo(line)
        (url, part)
      }).toMap
    val partition = hosts.get(hostname).getOrElse(-1)
    (partition, hosts)
  }
}
