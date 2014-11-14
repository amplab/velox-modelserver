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
  @(JsonProperty)("modelStorage")
  val modelStorageFactory: ModelStorageFactory = new ModelStorageFactory

  def build(env: Environment): Model[_, _] = {
    modelType match {
      case "MatrixFactorizationModel" => {
        // Build and manage the modelStorage
        val storage = modelStorageFactory.build[Long, FeatureVector](env, numFactors)
        val averageUser = Array.fill[Double](numFactors)(1.0)
        new MatrixFactorizationModel(numFactors, storage, averageUser)
      }
      case _ => {
        throw new IllegalArgumentException(s"Unknown model type: $modelType")
      }
    }
  }
}
