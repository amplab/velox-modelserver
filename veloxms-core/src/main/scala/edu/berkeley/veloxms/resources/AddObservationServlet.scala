package edu.berkeley.veloxms.resources

import java.util.concurrent.TimeUnit
import javax.servlet.http.{HttpServlet, HttpServletRequest, HttpServletResponse}

import com.codahale.metrics.Timer
import dispatch._
import edu.berkeley.veloxms._
import edu.berkeley.veloxms.models.Model
import edu.berkeley.veloxms.background.OnlineUpdateManager
import edu.berkeley.veloxms.util.{Utils, Logging}

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global
import scala.reflect._

class AddObservationServlet[T](
    onlineUpdateManager: OnlineUpdateManager[T],
    timer: Timer,
    modelName: String,
    partitionMap: Seq[String],
    hostname: String) extends HttpServlet with Logging {
  private val http = Http.configure(_.setAllowPoolingConnection(true).setFollowRedirects(true))
  val veloxPort = 8080
  val hosts = partitionMap.map {
    h => host(h, veloxPort).setContentType("application/json", "UTF-8")
  }

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

      val correctPartition = Utils.nonNegativeMod(uid.hashCode(), partitionMap.size)
      val output = if (partitionMap(correctPartition) == hostname) {
        val item: T = onlineUpdateManager.model.jsonToInput(context)
        onlineUpdateManager.addObservation(uid, item, score)
        "Successfully added observation"
      } else {
        val h = hosts(correctPartition)
        val forwardedReq = (h / "observe" / modelName)
            .POST << input.toString
        val httpReq = http(forwardedReq OK as.String)
        Await.result(httpReq, Duration(3000, TimeUnit.MILLISECONDS))
      }

      resp.setContentType("application/json")
      jsonMapper.writeValue(resp.getOutputStream, output)
    } finally {
      timeContext.stop()
    }
  }
}
