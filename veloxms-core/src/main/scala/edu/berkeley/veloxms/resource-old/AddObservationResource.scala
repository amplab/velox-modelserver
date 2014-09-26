package edu.berkeley.veloxms.resources

import edu.berkeley.veloxms.MovieRating;
import edu.berkeley.veloxms.storage.ModelStorage;
import io.dropwizard.jersey.params.LongParam;
import com.codahale.metrics.annotation.Timed;
import org.jblas.DoubleMatrix;
import org.jblas.Solve;

import javax.validation.Valid;
import javax.ws.rs.Consumes;
import javax.ws.rs.Produces;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.MediaType;
import java.util.Arrays;
import java.util.HashMap;



case class MatrixFactObservation (userId: UserID, itemId: Long, score: Double)

@Path("/observe/matrixfact")
@Consumes(Array(MediaType.APPLICATION_JSON))
@Produces(Array(MediaType.APPLICATION_JSON))
class MatrixFactorizationPredication(model: MatrixFactorizationModel,
  featureCache: FeatureCache[Long]) extends LazyLogging {


  @POST
  @Timed
  def observe(@Valid obs: MatrixFactObservation): Boolean = {

    logger.info(s"Adding rating for user: ${obs.userId}")
    val allObservationScores: Map[Long, Double] = model.modelStorage
      .getAllObservations(obs.userId) match {
        case Success(u) => u + (obs.itemId, obs.score)
        case Failure(thrown) => {
          logger.warn(s"No training data found for user ${obs.userId}")
          Map((obs.itemId, obs.score))
        }
      }

    val allItemFeatures: Map[Long, FeatureVector] = allObservationScores.map {
      case(itemId, _) => featureCache.getFeatures(itemId, featureCache)
    }

    val k = model.numFeatures
    val newUserWeights = OnlineUpdateUtils.updateUserWeights(
      allItemFeatures, allObservationScores, k)

    // TODO Write new observation, new user weights to Tachyon
    // TODO evaluate model quality
    true
  }

  @GET
  @Timed
  def retrainModel: Boolean = {


  }

}

object OnlineUpdateUtils {

  def updateUserWeights(allItemFeatures: Map[Long, FeatureVector],
    allObservationScores: Map[Long, Double], k: Int): WeightVector = {


    val itemFeaturesSum = DoubleMatrix.zeros(k, k)
    val itemScoreProductSum = DoubleMatrix.zeros(k)

    var i = 0

    val observedItems = allItemFeatures.keys().toList

    while (i < observedItems.size) {
      val currentItemId = observedItems(i)
      val currentFeatures = new DoubleMatrix(allItemFeatures.get(currentItemId))
      val product = currentFeatures.mmul(currentFeatures.transpose())
      itemFeaturesSum.addi(product)

      itemScoreProductSum.addi(currentFeatures.muli(allObservationScores.get(currentItemId)))
    }

    val regularization = DoubleMatrix.eye(k).muli(lambda*k)
    itemFeaturesSum.addi(regularization)
    newUserWeights = Solve.solve(itemFeaturesSum, itemScoreProductSum)
    newUserWeights.toArray()

  }


}




