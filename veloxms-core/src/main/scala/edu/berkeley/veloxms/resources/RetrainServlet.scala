package edu.berkeley.veloxms.resources

import javax.servlet.http.{HttpServletResponse, HttpServletRequest, HttpServlet}

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import com.codahale.metrics.Timer
import edu.berkeley.veloxms.models.Model
import edu.berkeley.veloxms._
import edu.berkeley.veloxms.util._
import edu.berkeley.veloxms.resources.internal.HDFSLocation
import dispatch._, Defaults._


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
    modelName: String,
    hostPartitionMap: Map[String, Int]) extends HttpServlet with Logging {

  override def doGet(req: HttpServletRequest, resp: HttpServletResponse) {
    val timeContext = timer.time()
    val veloxPort = 8080
    try {
      // coordinate retraining: returns false if retrain already going on

      val http = Http.configure(_.setAllowPoolingConnection(true).setFollowRedirects(true))
      /* BEGIN Pseudocode */
      // acquire retrain lock from etcd:
      // TODO should we wrap this in a try/finally to make sure lock get's released?
      logInfo(s"Starting retrain for model $modelName")
      val lockAcquired = etcdClient.acquireRetrainLock(modelName)
      val obsDataLocation = HDFSLocation(s"velox/$modelName/observations")
      val newModelLocation = HDFSLocation(s"velox/$modelName/retrained_model")

      if (lockAcquired) {
        val hosts = hostPartitionMap.map({
          case(h, _) => host(h, veloxPort).setContentType("application/json", "UTF-8")
        })

        // TODO Delete observation and new_model dirs if exist
        val writeRequests = hosts.map(
          h => {
            val req = (h / "writehdfs" / modelName)
              .POST << jsonMapper.writeValueAsString(obsDataLocation)
            http(req OK as.String)
          })

        val writeResponseFutures = Future.sequence(writeRequests)
        val writeResponses = Await.result(writeResponseFutures, Duration.Inf)
        logInfo(s"Write to hdfs responses: ${writeResponses.mkString("\n")}")
        model.retrainInSpark(
            s"spark://$sparkMaster:7077",
            s"hdfs://$sparkMaster:9000/${obsDataLocation.loc}",
            s"hdfs://$sparkMaster:9000/${newModelLocation.loc}")
        //
        // val loadModelRequests = hosts.map(
        //   h => {
        //     val req = (h / "loadmodel" / modelName)
        //       .POST << jsonMapper.writeValueAsString(newModelLocation)
        //     http(req OK as.String)
        //   })
        //
        // val loadResponseFutures = Future.sequence(loadModelRequests)
        // val loadResponses = Await.result(loadResponseFutures, Duration.Inf)
        // logInfo(s"Load new model responses: ${loadResponses.mkString("\n")}")
        val lockReleased = etcdClient.releaseRetrainLock(modelName)
        logInfo(s"released lock successfully: $lockReleased")
        jsonMapper.writeValue(resp.getOutputStream, "Success")
      } else {
        jsonMapper.writeValue(resp.getOutputStream, "Failed to acquire lock")
      }
      // model.retrainInSpark(sparkMaster)
      resp.setContentType("application/json");
    } finally {
      val lockReleased = etcdClient.releaseRetrainLock(modelName)
      logInfo(s"released lock successfully: $lockReleased")
      timeContext.stop()
    }
  }
}


