package edu.berkeley.veloxms.resources

import javax.ws.rs.Produces
import javax.ws.rs.GET
import javax.ws.rs.Path
import javax.ws.rs.core.MediaType
import com.codahale.metrics.annotation.Timed
import edu.berkeley.veloxms.util.Logging
import edu.berkeley.veloxms._
import edu.berkeley.veloxms.models._
import scala.collection.mutable

case class HitRate(hits: Long, misses: Long)
case class CacheHits(predictionCache: HitRate, featureCache: HitRate)
// case class AllCacheHits(name: String, results: List[CacheHits])


@Path("/cachehits")
@Produces(Array(MediaType.APPLICATION_JSON))
class CacheHitResource(models: Map[String, Model[_,_]]) extends Logging {
  @GET
  def getCacheHits: Map[String, CacheHits] = {
    val hits = new mutable.HashMap[String, CacheHits]()
    models.foreach { case (name, m) => {
        val predCache = m.predictionCache.getCacheHitRate
        val featureCache = m.featureCache.getCacheHitRate
        hits.put(name, CacheHits(HitRate(predCache._1, predCache._2), HitRate(featureCache._1, featureCache._2)))
      }
    }
    hits.toMap
  }
}
