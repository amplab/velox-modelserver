package edu.berkeley.veloxms

import edu.berkeley.veloxms.resources._
import edu.berkeley.veloxms.storage._
import io.dropwizard.Configuration
// import net.nicktelford.dropwizard.scala._
import io.dropwizard.setup.Bootstrap
import io.dropwizard.setup.Environment
// import org.slf4j.Logger
// import org.slf4j.LoggerFactory
import tachyon.TachyonURI
import tachyon.r.sorted.ClientStore
import scala.util.{Try, Success, Failure}
import com.fasterxml.jackson.annotation.JsonProperty
import org.hibernate.validator.constraints.NotEmpty
import com.typesafe.scalalogging._
import com.massrelevance.dropwizard.ScalaApplication
import com.massrelevance.dropwizard.bundles.ScalaBundle

import javax.validation.constraints.NotNull


case class VeloxConfiguration(
    @NotEmpty itemModelLoc: String,
    @NotEmpty userModelLoc: String,
    @NotEmpty ratingsLoc: String,
    @NotNull numFactors: Integer,
    sparkMaster: String,
    // whether to do preprocessing of dataset for testing purposes
    reloadTachyon: Boolean,
    rawDataLoc: String
    ) extends Configuration


object VeloxApplication extends ScalaApplication[VeloxConfiguration] with LazyLogging {

    override def getName = "velox model server"

    // val LOGGER = Logger(LoggerFactory.getLogger(VeloxApplication.class))

    override def initialize(bootstrap: Bootstrap[VeloxConfiguration]) {
        bootstrap.addBundle(new ScalaBundle)
        // init global state
    }

    override def run(config: VeloxConfiguration, env: Environment) {
        
        val userModel = TachyonUtils.getStore(config.userModelLoc) match {
            case Success(s) => s
            case Failure(f) => throw new RuntimeException(
                s"Couldn't initialize use model: ${f.getMessage}")
        }

        val itemModel = TachyonUtils.getStore(config.itemModelLoc) match {
            case Success(s) => s
            case Failure(f) => throw new RuntimeException(
                s"Couldn't initialize item model: ${f.getMessage}")
        }

        val ratings = TachyonUtils.getStore(config.ratingsLoc) match {
            case Success(s) => s
            case Failure(f) => throw new RuntimeException(
                s"Couldn't initialize use model: ${f.getMessage}")
        }

        val modelStorage = 
            new TachyonStorage[Array[Double]](userModel, itemModel, ratings, config.numFactors)
        val averageUser = Array.fill[Double](config.numFactors)(1.0)
        val matrixFactorizationModel =
            new MatrixFactorizationModel(config.numFactors, modelStorage, averageUser)

        val featureCache = new FeatureCache[Long](FeatureCache.tempBudget)

        env.jersey().register(new MatrixFactorizationPredictionResource(
            matrixFactorizationModel, featureCache, config.sparkMaster))
        // env.jersey().register(addRatings)
    }
}











