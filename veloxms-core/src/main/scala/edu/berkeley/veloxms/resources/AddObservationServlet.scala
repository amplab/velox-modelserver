package edu.berkeley.veloxms.resources

import javax.servlet.http.{HttpServlet, HttpServletRequest, HttpServletResponse}

import com.codahale.metrics.Timer
import edu.berkeley.veloxms._
import edu.berkeley.veloxms.background.OnlineUpdateManager
import edu.berkeley.veloxms.util.Logging

import scala.reflect._

class AddObservationServlet[T : ClassTag](onlineUpdateManager: OnlineUpdateManager[T], timer: Timer) extends HttpServlet with Logging {
  override def doPost(req: HttpServletRequest, resp: HttpServletResponse) {
    val timeContext = timer.time()
    try {
      val input = jsonMapper.readTree(req.getInputStream)
      require(input.has("uid"))
      require(input.has("context"))
      require(input.has("score"))
      val uid = input.get("uid").asLong
      val context = input.get("context")
      val score = input.get("score").asDouble()

      val item: T = fromJson(context)
      onlineUpdateManager.addObservation(uid, item, score)

      resp.setContentType("application/json")
      jsonMapper.writeValue(resp.getOutputStream, "Successfully added observation")
    } finally {
      timeContext.stop()
    }
  }
}
