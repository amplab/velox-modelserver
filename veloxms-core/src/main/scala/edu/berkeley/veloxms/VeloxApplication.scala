package edu.berkeley.veloxms

import edu.berkeley.veloxms.resources._
import edu.berkeley.veloxms.storage._
import edu.berkeley.veloxms.misc.WriteModelsResource
import io.dropwizard.Configuration
// import net.nicktelford.dropwizard.scala._
import io.dropwizard.setup.Bootstrap
import io.dropwizard.setup.Environment
import tachyon.TachyonURI
import tachyon.r.sorted.ClientStore
import scala.util.{Try, Success, Failure}
import com.fasterxml.jackson.annotation.JsonProperty
import org.hibernate.validator.constraints.NotEmpty
// import com.typesafe.scalalogging._
import com.massrelevance.dropwizard.ScalaApplication
import com.massrelevance.dropwizard.bundles.ScalaBundle
import javax.validation.constraints.NotNull

import edu.berkeley.veloxms.util.Logging

case class VeloxConfiguration(
    @NotEmpty itemModelLoc: String,
    @NotEmpty userModelLoc: String,
    @NotEmpty ratingsLoc: String,
    @NotNull numFactors: Integer,
    sparkMaster: String
    // whether to do preprocessing of dataset for testing purposes
    // reloadTachyon: Boolean,
    // rawDataLoc: String
    ) extends Configuration


object VeloxApplication extends ScalaApplication[VeloxConfiguration] with Logging {

    override def getName = "velox model server"

    // TODO I think this is fucked - look at Spark's Logging.scala to fix
    // val logger = LoggerFactory.getLogger(classOf[VeloxApplication])

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
        logInfo("got tachyon stores")

        val modelStorage = 
            new TachyonStorage(userModel, itemModel, ratings, config.numFactors)
            // new TachyonStorage[Array[Double]](userModel, itemModel, ratings, config.numFactors)
        val averageUser = Array.fill[Double](config.numFactors)(1.0)
        val matrixFactorizationModel =
            new MatrixFactorizationModel(config.numFactors, modelStorage, averageUser, config)

        val featureCache = new FeatureCache[Long](FeatureCache.tempBudget)

        env.jersey().register(new MatrixFactorizationPredictionResource(
            matrixFactorizationModel, featureCache))

        env.jersey().register(new MatrixFactorizationUpdateResource(
            matrixFactorizationModel, featureCache, config.sparkMaster))

        env.jersey().register(new WriteModelsResource)
        // env.jersey().register(addRatings)
    }
}











