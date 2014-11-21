package edu.berkeley.veloxms.models

import javax.validation.constraints.NotNull

import com.fasterxml.jackson.annotation.JsonProperty
import edu.berkeley.veloxms._
import edu.berkeley.veloxms.storage.ModelStorageFactory
import edu.berkeley.veloxms.util.Logging
import io.dropwizard.setup.Environment
import org.hibernate.validator.constraints.NotEmpty

/**
 * A storage factory to provide the configured model storage
 */
class ModelFactory extends Logging {
  @NotEmpty val modelType: String = ""
  @NotNull val numFactors: Integer = 50


  @NotNull
  @(JsonProperty)("storage")
  val modelStorageFactories: Map[String, ModelStorageFactory] = Map()

  def build(env: Environment): Model[_, _] = {
    modelType match {
      case "MatrixFactorizationModel" => {
        require(modelStorageFactories.contains("users"))
        require(modelStorageFactories.contains("items"))
        require(modelStorageFactories.contains("ratings"))

        // Build and manage the modelStorages
        val itemStore = modelStorageFactories.get("items").get.build[Long, FeatureVector](env)
        val userStore = modelStorageFactories.get("users").get.build[Long, WeightVector](env)
        val observationStore = modelStorageFactories.get("ratings").get.build[Long, Map[Long, Double]](env)
        val averageUser = Array.fill[Double](numFactors)(1.0)
        new MatrixFactorizationModel(numFactors, itemStore, userStore, observationStore, averageUser)
      }
      case "NewsgroupsModel" => {
        require(modelStorageFactories.contains("users"))
        require(modelStorageFactories.contains("ratings"))

        // Build and manage the modelStorages
        val userStore = modelStorageFactories.get("users").get.build[Long, WeightVector](env)
        val observationStore = modelStorageFactories.get("ratings").get.build[Long, Map[String, Double]](env)
        val averageUser = Array.fill[Double](numFactors)(1.0)
        new NewsgroupsModel(numFactors, userStore, observationStore, averageUser)
      }
      case _ => {
        throw new IllegalArgumentException(s"Unknown model type: $modelType")
      }
    }
  }
}
