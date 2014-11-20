package edu.berkeley.veloxms.resources

import javax.ws.rs.Produces
import javax.ws.rs.GET
import javax.ws.rs.Path
import javax.ws.rs.core.MediaType
import com.codahale.metrics.annotation.Timed
import edu.berkeley.veloxms.util.Logging

@Path("/models")
@Produces(Array(MediaType.TEXT_PLAIN))
class ModelListResource(modelNames: Seq[String]) extends Logging {

  @GET
  @Timed
  def listModels: String = {
    "Registered Models: " + modelNames.mkString(",")
  }
}
