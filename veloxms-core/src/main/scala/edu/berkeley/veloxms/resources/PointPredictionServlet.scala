/**
 * Resource class to handle requests of form predict(uid, x: Data)
 *
 */


package edu.berkeley.veloxms.resources


import java.util.concurrent.TimeUnit
import javax.servlet.http.{HttpServletResponse, HttpServletRequest, HttpServlet}

import com.codahale.metrics.Timer
import dispatch._
import edu.berkeley.veloxms._
import edu.berkeley.veloxms.models.Model
import edu.berkeley.veloxms.util.Utils

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global
import scala.reflect._

class PointPredictionServlet[T](
    model: Model[T],
    timer: Timer,
    partitionMap: Seq[String],
    hostname: String) extends HttpServlet {
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
      val uid = input.get("uid").asLong
      val context = input.get("context")

      val correctPartition = Utils.nonNegativeMod(uid.hashCode(), partitionMap.size)
      val output = if (partitionMap(correctPartition) == hostname) {
        // val item: T = fromJson[T](context)
        val item: T = model.jsonToInput(context)
        println(item.getClass)
        model.predict(uid, item, model.currentVersion)
      } else {
        val h = hosts(correctPartition)
        val forwardedReq = (h / "predict" / model.modelName)
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

