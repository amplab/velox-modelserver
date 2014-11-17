package edu.berkeley.veloxms.resources

import javax.servlet.http.{HttpServletResponse, HttpServletRequest, HttpServlet}

import com.codahale.metrics.Timer
import edu.berkeley.veloxms.models.Model
import edu.berkeley.veloxms._

class RetrainServlet(model: Model[_, _], sparkMaster: String, timer: Timer) extends HttpServlet {
  override def doPost(req: HttpServletRequest, resp: HttpServletResponse) {
    val timeContext = timer.time()
    try {
      model.retrainInSpark(sparkMaster)
      resp.setContentType("application/json");
      jsonMapper.writeValue(resp.getOutputStream, "Success")
    } finally {
      timeContext.stop()
    }
  }
}
