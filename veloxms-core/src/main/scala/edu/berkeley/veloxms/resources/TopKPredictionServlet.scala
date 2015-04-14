package edu.berkeley.veloxms.resources

import com.codahale.metrics.Timer

import edu.berkeley.veloxms.models.Model
import edu.berkeley.veloxms._

import javax.servlet.http.{HttpServlet, HttpServletRequest, HttpServletResponse}

class TopKPredictionServlet(model: Model[_, _], timer: Timer) extends HttpServlet {
  override def doPost(req: HttpServletRequest, resp: HttpServletResponse) = {
    val timeContext = timer.time()
    try {
      val input = jsonMapper.readTree(req.getInputStream)
      require(input.has("uid"))
      require(input.has("k"))
      require(input.has("context"))

      val uid: Long = input.get("uid").asLong()
      val k: Int = input.get("k").asInt()
      val context = input.get("context")

      val topK = model.predictTopK(uid, k, context, model.currentVersion)

      resp.setContentType("application/json")
      jsonMapper.writeValue(resp.getOutputStream, topK)
    } finally {
      timeContext.stop()
    }
  }
}