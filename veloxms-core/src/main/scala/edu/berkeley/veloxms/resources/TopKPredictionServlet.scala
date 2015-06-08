package edu.berkeley.veloxms.resources

import java.util.concurrent.TimeUnit

import com.codahale.metrics.Timer
import dispatch._
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import edu.berkeley.veloxms.models.Model
import edu.berkeley.veloxms._

import javax.servlet.http.{HttpServlet, HttpServletRequest, HttpServletResponse}

import edu.berkeley.veloxms.util.Utils
import scala.concurrent.duration.Duration
import scala.reflect._

class TopKPredictionServlet[T](
    model: Model[T],
    timer: Timer,
    partitionMap: Seq[String],
    hostname: String) extends HttpServlet {
  private val http = Http.configure(_.setAllowPoolingConnection(true).setFollowRedirects(true))
  val veloxPort = 8080
  val hosts = partitionMap.map {
    h => host(h, veloxPort).setContentType("application/json", "UTF-8")
  }

  override def doPost(req: HttpServletRequest, resp: HttpServletResponse) = {
    val timeContext = timer.time()
    try {
      val input = jsonMapper.readTree(req.getInputStream)
      require(input.has("uid"))
      require(input.has("k"))
      require(input.has("context"))

      val uid: Long = input.get("uid").asLong()

      val correctPartition = Utils.nonNegativeMod(uid.hashCode(), partitionMap.size)

      val topK = if (partitionMap(correctPartition) == hostname) {
        val k: Int = input.get("k").asInt()
        val context = input.get("context")

        val candidateSet: Array[T] = model.jsonArrayToInput(context)
        model.predictTopK(uid, k, candidateSet, model.currentVersion)
      } else {
        val h = hosts(correctPartition)
        val forwardedReq = (h / "predict_top_k" / model.modelName)
            .POST << input.toString
        val httpReq = http(forwardedReq OK as.String)
        Await.result(httpReq, Duration(3000, TimeUnit.MILLISECONDS))
      }

      resp.setContentType("application/json")
      jsonMapper.writeValue(resp.getOutputStream, topK)
    } finally {
      timeContext.stop()
    }
  }
}
