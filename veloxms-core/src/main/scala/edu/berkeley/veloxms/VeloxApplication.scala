package edu.berkeley.veloxms

import edu.berkeley.veloxms.resources._
import edu.berkeley.veloxms.storage._
import io.dropwizard.Configuration

import io.dropwizard.setup.Bootstrap
import io.dropwizard.setup.Environment
import com.fasterxml.jackson.annotation.JsonProperty
import com.massrelevance.dropwizard.ScalaApplication
import com.massrelevance.dropwizard.bundles.ScalaBundle
import javax.validation.constraints.NotNull

import edu.berkeley.veloxms.util.Logging

class VeloxConfiguration extends Configuration {
  @NotNull val numFactors: Integer = 50

  val sparkMaster: String = "NoSparkMaster"
  @(JsonProperty)("modelStorage")
  val modelStorageFactory: ModelStorageFactory = new ModelStorageFactory

  // sparkMaster: String
    // whether to do preprocessing of dataset for testing purposes
    // reloadTachyon: Boolean,
    // rawDataLoc: String
}


object VeloxApplication extends ScalaApplication[VeloxConfiguration] with Logging {

    override def getName = "velox model server"

    // TODO I think this is fucked - look at Spark's Logging.scala to fix
    // val logger = LoggerFactory.getLogger(classOf[VeloxApplication])

    override def initialize(bootstrap: Bootstrap[VeloxConfiguration]) {
        bootstrap.addBundle(new ScalaBundle)
        // init global state
    }

    override def run(conf: VeloxConfiguration, env: Environment) {
        val modelStorage = conf.modelStorageFactory.build(env, conf.numFactors)

        val averageUser = Array.fill[Double](conf.numFactors)(1.0)
        val featureCache = new FeatureCache[Long](FeatureCache.tempBudget)
        val matrixFactorizationModel =
            new MatrixFactorizationModel(conf.numFactors, modelStorage, averageUser, conf)

        env.jersey().register(new MatrixFactorizationPredictionResource(
            matrixFactorizationModel, featureCache))

        env.jersey().register(new MatrixFactorizationUpdateResource(
            matrixFactorizationModel, featureCache, conf.sparkMaster))
    }
}


