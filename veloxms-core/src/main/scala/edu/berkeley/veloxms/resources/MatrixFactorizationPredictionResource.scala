/**
 * Resource class to handle requests of form predict(uid, x: Data)
 *
 */


package edu.berkeley.veloxms.resources

import edu.berkeley.veloxms.storage.ModelStorage
import edu.berkeley.veloxms._
import com.codahale.metrics.annotation.Timed
// import net.nicktelford.dropwizard.scala.jersey.LongParam
// import net.nicktelford.dropwizard.scala.jersey.IntParam
// import net.nicktelford.dropwizard.scala.jersey.BooleanParam
// import org.slf4j.Logger
// import org.slf4j.LoggerFactory

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
import com.typesafe.scalalogging._
import com.massrelevance.dropwizard.scala.params.{LongParam, IntParam, BooleanParam}

@Path("/predict-matrixfact")
@Consumes(Array(MediaType.APPLICATION_JSON))
@Produces(Array(MediaType.APPLICATION_JSON))
// TODO figure out how to make map of models of different types
class MatrixFactorizationPredictionResource(model: MatrixFactorizationModel,
    featureCache: FeatureCache[Long]) extends LazyLogging {

  @POST
  @Timed
  def predict(
      @QueryParam("model") modelId: IntParam,
      @QueryParam("user") userId: LongParam,
      data: Array[Byte])
      : (Array[Byte], Double) = {
    // val model = models.get(modelId.value)
    val item = model.deserializeInput(data)
    val features = featureCache.getItem(item) match {
      case Some(f) => f
      case None => {
        val f = model.computeFeatures(item)
        featureCache.addItem(item, f)
        f
      }
    }

    val weightVector = model.getWeightVector(userId.value)
    var i = 0
    var score = 0.0
    while (i < model.numFeatures) {
      score += features(i)*weightVector(i)
    }
    (data, score)
  }

}




