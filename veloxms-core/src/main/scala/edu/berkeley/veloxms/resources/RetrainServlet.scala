package edu.berkeley.veloxms.resources

import javax.servlet.http.{HttpServletResponse, HttpServletRequest, HttpServlet}

import edu.berkeley.veloxms.models.Model
import edu.berkeley.veloxms._

class RetrainServlet(model: Model[_, _], sparkMaster: String) extends HttpServlet {
  override def doPost(req: HttpServletRequest, resp: HttpServletResponse) {
    model.retrainInSpark(sparkMaster)
    resp.setContentType("application/json");
    jsonMapper.writeValue(resp.getOutputStream, "Success")
  }
}
