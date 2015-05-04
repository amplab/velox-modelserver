package edu.berkeley.veloxms.resources

import javax.servlet.http.{HttpServlet, HttpServletRequest, HttpServletResponse}

import com.codahale.metrics.Timer
import edu.berkeley.veloxms._
import edu.berkeley.veloxms.background.BatchRetrainManager
import edu.berkeley.veloxms.util._


class RetrainServlet[T](
    batchRetrainManager: BatchRetrainManager[T],
    timer: Timer,
    etcdClient: EtcdClient) extends HttpServlet with Logging {

  override def doGet(request: HttpServletRequest, resp: HttpServletResponse) {
    val timeContext = timer.time()
    val result = batchRetrainManager.batchTrain()
    jsonMapper.writeValue(resp.getOutputStream, result)
    resp.setContentType("application/json")
    timeContext.stop()
  }
}


