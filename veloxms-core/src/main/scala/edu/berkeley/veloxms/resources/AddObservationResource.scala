package edu.berkeley.veloxms.resources

import edu.berkeley.veloxms.storage.ModelStorage
import io.dropwizard.jersey.params.LongParam
import com.codahale.metrics.annotation.Timed
import org.jblas.DoubleMatrix
import org.jblas.Solve
import edu.berkeley.veloxms._
// import com.typesafe.scalalogging._
import javax.validation.Valid
import javax.ws.rs.Consumes
import javax.ws.rs.Produces
import javax.ws.rs.POST
import javax.ws.rs.GET
import javax.ws.rs.Path
import javax.ws.rs.PathParam
import javax.ws.rs.core.MediaType
import scala.util.{Try, Success, Failure}

import org.slf4j.Logger
import org.slf4j.LoggerFactory



case class MatrixFactObservation (userId: UserID, itemId: Long, score: Double)

@Path("/observe/matrixfact")
@Consumes(Array(MediaType.APPLICATION_JSON))
@Produces(Array(MediaType.APPLICATION_JSON))
class MatrixFactorizationUpdateResource(model: MatrixFactorizationModel,
  featureCache: FeatureCache[Long], sparkMaster: String) {

  val logger = Logger(LoggerFactory.getLogger(MatrixFactorizationUpdateResource.class))


  @POST
  @Timed
  def observe(@Valid obs: MatrixFactObservation): Boolean = {

    logger.info(s"Adding rating for user: ${obs.userId}")
    val allObservationScores: Map[Long, Double] = model.modelStorage
      .getAllObservations(obs.userId) match {
        case Success(u) => u + ((obs.itemId, obs.score))
        case Failure(thrown) => {
          logger.warn(s"No training data found for user ${obs.userId}")
          Map((obs.itemId, obs.score))
        }
      }

    val allItemFeatures: Map[Long, FeatureVector] = allObservationScores.map {
      case(itemId, _) => (itemId, model.getFeatures(itemId, featureCache))
    }

    val k = model.numFeatures
    val oldUserWeights = model.getWeightVector(obs.userId)
    val newUserWeights = OnlineUpdateUtils.updateUserWeights(
      allItemFeatures, allObservationScores, k)
    logger.info(s"Old weight: (${oldUserWeights.mkString(",")})")
    logger.info(s"New weight: (${newUserWeights.mkString(",")})")


    // TODO Write new observation, new user weights to Tachyon
    // TODO evaluate model quality
    true
  }

  @GET
  @Timed
  def retrainModel: Boolean = {
   // TODO call model.retrainInSpark(sparkMaster)
   model.retrainInSpark()
   true
  }

}

object OnlineUpdateUtils {


  def updateUserWeights(allItemFeatures: Map[Long, FeatureVector],
    allObservationScores: Map[Long, Double], k: Int): WeightVector = {


    val itemFeaturesSum = DoubleMatrix.zeros(k, k)
    val itemScoreProductSum = DoubleMatrix.zeros(k)

    var i = 0

    val observedItems = allItemFeatures.keys.toList

    while (i < observedItems.size) {
      val currentItemId = observedItems(i)
      // TODO error handling
      val currentFeaturesArray = allItemFeatures.get(currentItemId) match {
        case Some(f) => f
        case None => throw new Exception(s"Missing features in online update -- item: $currentItemId")
      }
      val currentFeatures = new DoubleMatrix(currentFeaturesArray)
      val product = currentFeatures.mmul(currentFeatures.transpose())
      itemFeaturesSum.addi(product)

      val obsScore = allObservationScores.get(currentItemId) match {
        case Some(o) => o
        case None => throw new Exception(s"Missing rating in online update -- item: $currentItemId")

      }
      itemScoreProductSum.addi(currentFeatures.muli(obsScore))
      i += 1

    }

    val regularization = DoubleMatrix.eye(k).muli(MatrixFactorizationModel.lambda*k)
    itemFeaturesSum.addi(regularization)
    val newUserWeights = Solve.solve(itemFeaturesSum, itemScoreProductSum)
    newUserWeights.toArray()

  }


}




