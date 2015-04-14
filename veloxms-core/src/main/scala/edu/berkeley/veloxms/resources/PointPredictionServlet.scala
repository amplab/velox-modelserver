/**
 * Resource class to handle requests of form predict(uid, x: Data)
 *
 */


package edu.berkeley.veloxms.resources


import javax.servlet.http.{HttpServletResponse, HttpServletRequest, HttpServlet}

import com.codahale.metrics.Timer
import edu.berkeley.veloxms._
import com.codahale.metrics.annotation.Timed
import edu.berkeley.veloxms.models.Model

class PointPredictionServlet(model: Model[_, _], timer: Timer) extends HttpServlet {
  override def doPost(req: HttpServletRequest, resp: HttpServletResponse) {
    val timeContext = timer.time()
    try {

      val input = jsonMapper.readTree(req.getInputStream)
      require(input.has("uid"))
      require(input.has("context"))
      val uid = input.get("uid").asLong
      val context = input.get("context")
      val score = model.predict(uid, context, model.currentVersion)
      resp.setContentType("application/json")
      jsonMapper.writeValue(resp.getOutputStream, score)
    } finally {
      timeContext.stop()
    }
  }
}

