/**
 * Resource class to handle requests of form predict(uid, x: Data)
 *
 */


package edu.berkeley.veloxms.resources


import javax.servlet.http.{HttpServletResponse, HttpServletRequest, HttpServlet}

import edu.berkeley.veloxms._
import com.codahale.metrics.annotation.Timed
import edu.berkeley.veloxms.models.Model

class PointPredictionServlet(model: Model[_, _]) extends HttpServlet {
  override def doPost(req: HttpServletRequest, resp: HttpServletResponse) {

    val input = jsonMapper.readTree(req.getInputStream)
    require(input.has("uid"))
    require(input.has("context"))
    val uid = input.get("uid").asLong
    val context = input.get("context")

    val score = model.predict(uid, context)

    resp.setContentType("application/json");
    jsonMapper.writeValue(resp.getOutputStream, score)
  }
}
