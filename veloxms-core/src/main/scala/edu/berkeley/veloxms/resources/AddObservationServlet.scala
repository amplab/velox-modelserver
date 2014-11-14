package edu.berkeley.veloxms.resources

import javax.servlet.http.{HttpServlet, HttpServletRequest, HttpServletResponse}

import edu.berkeley.veloxms._
import edu.berkeley.veloxms.models.Model
import edu.berkeley.veloxms.util.Logging

class AddObservationServlet(model: Model[_, _], sparkMaster: String) extends HttpServlet with Logging {
  override def doPost(req: HttpServletRequest, resp: HttpServletResponse) {

    val input = jsonMapper.readTree(req.getInputStream)
    require(input.has("uid"))
    require(input.has("context"))
    require(input.has("score"))
    val uid = input.get("uid").asLong
    val context = input.get("context")
    val score = input.get("score").asDouble()

    model.addObservation(uid, context, score)
    
    resp.setContentType("application/json");
    jsonMapper.writeValue(resp.getOutputStream, "Successfully added observation")
  }
}
