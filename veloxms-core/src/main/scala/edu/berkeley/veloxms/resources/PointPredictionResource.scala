/**
 * Resource class to handle requests of form predict(uid, x: Data)
 *
 */


package edu.berkeley.veloxms.resources

import edu.berkeley.veloxms.storage.ModelStorage
import edu.berkeley.veloxms._
import io.dropwizard.jersey.params.LongParam
import com.codahale.metrics.annotation.Timed
// import org.slf4j.Logger
// import org.slf4j.LoggerFactory
import edu.berkeley.veloxms.BaseItemSet

import javax.validation.Valid
import javax.ws.rs.GET
import javax.ws.rs.POST
import javax.ws.rs.Path
import javax.ws.rs.PathParam
import javax.ws.rs.QueryParam
import javax.ws.rs.Consumes
import javax.ws.rs.Produces
import javax.ws.rs.core.MediaType
import java.util._
import com.typesafe.scalalogging.LazyLogging

@Path("/predict")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
class PointPredictionResource(models: Map[Int, Model],
    featureCache: FeatureCache(budget)) extends LazyLogging {

  @POST
  @Timed
  def predict(@QueryParam("model") modelId: IntParam,
      @QueryParam("user") userId: LongParam,
      data: Array[Byte]) : (Array[Byte], Float) = {
    val model = models.get(modelId)
    val features = featureCache.getItem(data) match {
      case Some(f) => f
      case None => {
        val f = model.computeFeatures(item)
        featureCache.addItem(data, f)
        f
      }
    }

    val weightVector = model.getWeightVector(userId)
    var i = 0
    var score = 0
    while (i < model.numFeature) {
      score += features[i]*weightVector
    }
    score
  }

}


object PointPredictionResource {

  // Totally arbitrary placeholder until we figure out what
  // a cache budget means
  val budget = 100

}


