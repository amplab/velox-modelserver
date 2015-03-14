package edu.berkeley.veloxms.resources

import javax.servlet.http.{HttpServletResponse, HttpServletRequest, HttpServlet}

import com.codahale.metrics.Timer
import edu.berkeley.veloxms.models.Model
import edu.berkeley.veloxms._
import edu.berkeley.veloxms.util._


/**
 * JK, I can just have the server coordinate everything:
 *  Server:
 *    - Acquire global retrain lock (lock per model) from etcd
 *    - Request servers write obs to HDFS - servers respond to request when done
 *        * Don't forget, this should be done for the retrain master as well
 *    - Retrain in Spark
 *    - When Spark retrain done, request servers load new model - servers respond when done
 *        * Don't forget, this should be done for the retrain master as well
 *    - Release retrain lock
 *
 *
 *
 */

class RetrainServlet(
    model: Model[_, _],
    sparkMaster: String,
    timer: Timer,
    etcdClient: EtcdClient,
    modelName: String) extends HttpServlet with Logging {

  override def doGet(req: HttpServletRequest, resp: HttpServletResponse) {
    val timeContext = timer.time()
    try {
      // coordinate retraining: returns false if retrain already going on

      /* BEGIN Pseudocode */
      // acquire retrain lock from etcd:
      // TODO should we wrap this in a try/finally to make sure lock get's released?
      logInfo(s"Starting retrain for model $modelName")
      val lockAcquired = etcdClient.acquireRetrainLock(modelName)
      if (lockAcquired) {
        Thread.sleep(5000) 
        val lockReleased = etcdClient.releaseRetrainLock(modelName)

        jsonMapper.writeValue(resp.getOutputStream, "Success\n")
      } else {
        jsonMapper.writeValue(resp.getOutputStream, "Failed to acquire lock\n")
      }


      // model.retrainInSpark(sparkMaster)
      resp.setContentType("application/json");
    } finally {
      timeContext.stop()
    }
  }
}


