package edu.berkeley.veloxms.util

// import scala.concurrent._
// import ExecutionContext.Implicits.global
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.util.{Success, Failure}
import com.fasterxml.jackson.databind.ObjectMapper
import com.ning.http.client.Response
import com.ning.http.client.extra.ThrottleRequestFilter
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.ning.http.client.{Request, Response, AsyncCompletionHandler, AsyncHandler, HttpResponseStatus}
import edu.berkeley.veloxms._
import scala.util.control.NonFatal

import dispatch._, Defaults._

/**
 * Responsibilities of etcd:
 *    - global locks (to avoid concurrent feature retraining in Spark)
 *    - cluster membership (TODO)
 *
 */


// TODO need to do error handling

object EtcdConstants {

  val basePath = "/v2/keys/"
  val LOCKED = "locked"
  val UNLOCKED = "unlocked"
  val retrainLock = "retrain_lock"

}

// https://github.com/dispatch/reboot/blob/f09f246cd12c0bb9212f92667b4cd49810f84454/core/src/main/scala/handlers.scala
trait OkAnd404Handler[T] extends AsyncHandler[T] {
  abstract override def onStatusReceived(status: HttpResponseStatus) = {
    if (status.getStatusCode / 100 == 2 || status.getStatusCode == 404) {
      super.onStatusReceived(status)
    } else {
      throw StatusCode(status.getStatusCode)
    }
  }
}

class OkAnd404FunctionHandler[T](f: Response => T)
extends FunctionHandler[T](f) with OkAnd404Handler[T]



// Made a class (vs object) for testing purposes)
class DispatchUtil extends Logging {

  val http = Http.configure(_.setAllowPoolingConnection(true)
                            .setFollowRedirects(true))

  private[this] def okAnd404ResponseHandler(response: Response): (Int, String) = {
    (response.getStatusCode, response.getResponseBody)
  }

  def sendRequestAccept404(req: Req): Future[(Int, String)] = {
    // val request = req.toRequest
    // logInfo(s"Sending req (accepts 404s) of type ${request.getMethod} with body ${request.getStringData}")
    http((req.toRequest, new OkAnd404FunctionHandler(okAnd404ResponseHandler)))
  }

  def sendRequest(req: Req): Future[String] = {
    // val request = req.toRequest
    // logInfo(s"Sending req of type ${request.getMethod} with body ${request.getStringData}")
    http(req OK as.String )
  }

}


// for now, etcdServer and hostname are probably the same
class EtcdClient(etcdHost: String, etcdPort: Int, hostname: String, dispatchUtil: DispatchUtil) extends Logging {

  val etcdServer = host(etcdHost, etcdPort).setContentType("application/x-www-form-urlencoded", "UTF-8")
  
  /**
   * This is a blocking call
   */
  def acquireRetrainLock(modelName: String): Boolean = {

    // case 0: lock didn't exist yet => error status and error code = 100
    //    atomic CAS on prevExists=false
    // case 1: lock exists, prevNode.value == "unlocked"
    //    atomic CAS on prevValue=False, prevIndex=prevNode.index
    // case 2: lock exists, prevNode.value == "locked", return false
    
    logWarning(s"Attempting to acquire $modelName retrain lock")

    var lockAcquired = false
    try {
      val checkUnlockedReq = (etcdServer / EtcdConstants.basePath / modelName / EtcdConstants.retrainLock).GET
      
      val checkUnlockedResponse = dispatchUtil.sendRequestAccept404(checkUnlockedReq)
      // Block until future completes
      val (checkUnlockedStatus, checkUnlockedJson) = Await.result(checkUnlockedResponse, Duration.Inf)
      lockAcquired = if (checkUnlockedStatus == 404) {
        lockCheckFailed(modelName, checkUnlockedJson)
      } else {
        lockCheckSucceeded(modelName, checkUnlockedJson)
      }
    } catch {
      // handle exception; note that NonFatal does not match InterruptedException
      case NonFatal(e) => {
        logWarning(s"Problem acquiring $modelName lock: ${e.getMessage()}")
        lockAcquired = false
      }
      case e: InterruptedException => {
        logWarning(s"Problem acquiring $modelName lock: ${e.getMessage()}")
        lockAcquired = false
      }
    }
    lockAcquired
  }

  /**
   * Case 0
   * @throws RuntimeException
   */
  private[this] def lockCheckFailed(modelName: String, checkUnlockedJson: String): Boolean = {
    val checkUnlockedError = jsonMapper.readValue(checkUnlockedJson, classOf[EtcdError])
    if (checkUnlockedError.errorCode == 100) {
      // CASE 0: lock doesn't exist yet
      // create lock
      val body = s"value=$hostname"
      // val body = s"""value=test"""
      // val body = jsonMapper.writeValueAsString(Map("value" -> hostname))
      logInfo(s"message body: $body")
      val acquireLockReq =
        (etcdServer / EtcdConstants.basePath / modelName / EtcdConstants.retrainLock)
          .PUT
          .setContentType("application/x-www-form-urlencoded", "UTF-8")
          .setBody(body) <<? Map("prevExist" -> "false")

      // acquireLockReq.setBody(body)

      val acquireLockResponse = dispatchUtil.sendRequest(acquireLockReq)
      val acquireLockJson = Await.result(acquireLockResponse, Duration.Inf)
      val acquireLockResult = jsonMapper.readValue(acquireLockJson, classOf[EtcdResponse])
      logInfo(s"Successfully acquired $modelName retrain lock")
      true
    } else {
      logWarning(s"Problem acquiring $modelName retrain lock. " +
        s"${checkUnlockedError.errorCode} ${checkUnlockedError.message}: ${checkUnlockedError.cause}")
      false
    }
  }

  private[this] def lockCheckSucceeded(modelName: String, checkUnlockedJson: String): Boolean = {
    val etcdResp = jsonMapper.readValue(checkUnlockedJson, classOf[EtcdResponse])
    if (etcdResp.node.value.get == EtcdConstants.UNLOCKED) {
      // CASE 1: lock available, atomic compare-and-swap
      val body = s"value=$hostname"
      val acquireLockReq =
        (etcdServer / EtcdConstants.basePath / modelName / EtcdConstants.retrainLock)
          .PUT 
          .setContentType("application/x-www-form-urlencoded", "UTF-8")
          .setBody(body) <<? Map(
          "prevIndex" -> s"${etcdResp.node.modifiedIndex}",
          "prevValue" -> etcdResp.node.value.get)

      val acquireLockResponse = dispatchUtil.sendRequest(acquireLockReq)
      val acquireLockJson = Await.result(acquireLockResponse, Duration.Inf)
      val acquireLockResult = jsonMapper.readValue(acquireLockJson, classOf[EtcdResponse])
      logInfo(s"Successfully acquired $modelName retrain lock")
      true
    } else {
      // CASE 2: lock unavailable => return false
      logWarning(s"Cannot acquire lock for $modelName. It's currently held by ${etcdResp.node.value.get}")
      false
    }
  }


  def releaseRetrainLock(modelName: String): Boolean = {
    var lockReleased = false
    try {

      val checkLockedReq = (etcdServer / EtcdConstants.basePath / modelName / EtcdConstants.retrainLock).GET
      val checkLockedResponse = dispatchUtil.sendRequest(checkLockedReq)
      val checkLockedJson = Await.result(checkLockedResponse, Duration.Inf)
      val lockStatus = jsonMapper.readValue(checkLockedJson, classOf[EtcdResponse])

      lockReleased = if (lockStatus.node.value.get == hostname) {
        // means we hold the lock and can release it
        val body = s"value=${EtcdConstants.UNLOCKED}"
        val releaseLockReq =
          (etcdServer / EtcdConstants.basePath / modelName / EtcdConstants.retrainLock)
            .PUT
            .setBody(body) <<? Map(
              "prevIndex" -> s"${lockStatus.node.modifiedIndex}",
              "prevValue" -> lockStatus.node.value.get)
        val releaseLockResponse = dispatchUtil.sendRequest(releaseLockReq)
        val releaseLockJson = Await.result(releaseLockResponse, Duration.Inf)
        val releaseLockResult = jsonMapper.readValue(releaseLockJson, classOf[EtcdResponse])
        logInfo(s"Successfully released $modelName retrain lock")
        true
      } else {
        // we don't own the lock, and so we don't change the value
        logWarning(s"Someone else owns $modelName retrain lock: belongs to ${lockStatus.node.value.get}")
        false
      }
    } catch {
      // handle exception; note that NonFatal does not match InterruptedException
      case NonFatal(e) => {
        logWarning(s"Problem releasing $modelName lock: ${e.getMessage()}")
        lockReleased = false
      }
      case e: InterruptedException => {
        logWarning(s"Problem releasing $modelName lock: ${e.getMessage()}")
        lockReleased = false
      }
    }
    lockReleased
  }

}
