package edu.berkeley.veloxms.resources

import java.util.Date
import javax.servlet.http.{HttpServletResponse, HttpServletRequest, HttpServlet}

import org.apache.spark.SparkContext

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import com.codahale.metrics.Timer
import edu.berkeley.veloxms.models.Model
import edu.berkeley.veloxms._
import edu.berkeley.veloxms.util._
import edu.berkeley.veloxms.resources.internal.{LoadModelParameters, HDFSLocation}
import dispatch._, Defaults._
import org.apache.hadoop.conf._
import org.apache.hadoop.fs._



/**
 *  Server:
 *    - Acquire global retrain lock (lock per model) from etcd
 *    - Tell servers to write obs to HDFS - servers respond to request when done
 *        * Don't forget, this should be done for the retrain master as well
 *    - Retrain in Spark
 *    - When Spark retrain done, request servers load new model - servers respond when done
 *        * Don't forget, this should be done for the retrain master as well
 *    - Release retrain lock
 *
 */

class RetrainServlet(
    model: Model[_, _],
    timer: Timer,
    etcdClient: EtcdClient,
    hostPartitionMap: Map[String, Int]) extends HttpServlet with Logging {

  override def doGet(request: HttpServletRequest, resp: HttpServletResponse) {
    val timeContext = timer.time()
    val result = model.batchTrain()
    jsonMapper.writeValue(resp.getOutputStream, result)
    resp.setContentType("application/json");
    timeContext.stop()
  }
}


