package edu.berkeley.veloxms.misc


import org.apache.commons.io.IOUtils
import org.apache.commons.lang3.SerializationUtils
import tachyon.TachyonURI
import tachyon.Pair
import tachyon.r.sorted.ClientStore
import tachyon.r.sorted.{Utils => TUtils}
import edu.berkeley.veloxms.storage.TachyonUtils
import scala.collection.immutable.TreeMap
import scala.io.Source
import org.apache.spark.mllib.recommendation.Rating
import com.typesafe.scalalogging._
import edu.berkeley.veloxms._
import io.dropwizard.jersey.params.LongParam
import com.codahale.metrics.annotation.Timed
import javax.validation.Valid
import javax.ws.rs.Consumes
import javax.ws.rs.Produces
import javax.ws.rs.POST
import javax.ws.rs.Path
import javax.ws.rs.PathParam
import javax.ws.rs.core.MediaType
import scala.collection.immutable.HashMap


/**
 * Utility for writing existing models in plaintext on the
 * local filesystem to Spark. Used only to setup environment for
 * testing purposes.
 */

/*
curl -H "Content-Type: application/json" -d '{
  "itemsDst":"tachyon://ec2-54-234-135-209.compute-1.amazonaws.com:19998/item-model",
"usersDst":"tachyon://ec2-54-234-135-209.compute-1.amazonaws.com:19998/user-model",
"obsDst":"tachyon://ec2-54-234-135-209.compute-1.amazonaws.com:19998/movie-ratings",
"itemsSrc":"/home/ubuntu/data/product_model.txt",
"usersSrc":"/home/ubuntu/data/user_model.txt",
"obsSrc":"/home/ubuntu/data/ratings.dat",
"partition":0}' http://localhost:8080/misc/prep-tachyon

*/

case class ModelLocations(
  itemsDst: String,
  usersDst: String,
  obsDst: String,
  itemsSrc: String,
  usersSrc: String,
  obsSrc: String,
  partition: Int
)


@Path("/misc/prep-tachyon")
@Consumes(Array(MediaType.APPLICATION_JSON))
@Produces(Array(MediaType.APPLICATION_JSON))
class WriteModelsResource extends LazyLogging {


  @POST
  @Timed
  def writeAllToTachyon(@Valid locs: ModelLocations): Boolean = {
    logger.info("Writing items")
    writeMapToTachyon(readModel(locs.itemsSrc), locs.itemsDst, locs.partition)
    logger.info("Writing users")
    writeMapToTachyon(readModel(locs.usersSrc), locs.usersDst, locs.partition)
    logger.info("Writing observations")
    writeMapToTachyon(readObservations(locs.obsSrc), locs.obsDst, locs.partition)
    true
  }

  // Read MatrixFactorizationModel
  def readModel(modelLoc: String): TreeMap[Array[Byte], Array[Byte]] = {
    val model = Source.fromFile(modelLoc).getLines.map( (line) => {
      val splits = line.split(",")
      val key = splits(0).toLong
      val factors: Array[Double] = splits.slice(1, splits.size).map(_.toDouble)
      (TachyonUtils.long2ByteArr(key), SerializationUtils.serialize(factors))
    })

    val sortedModel = TreeMap(model.toArray:_*)(ByteOrdering)
    sortedModel
  }

  def writeMapToTachyon(map: TreeMap[Array[Byte], Array[Byte]], loc: String,
      partition: Int = 0) {
    val store = ClientStore.createStore(new TachyonURI(loc))
    store.createPartition(partition)
    map.foreach( {case (key, value) => store.put(partition, key, value) })
    store.closePartition(partition)
  }

  def readObservations(obsLoc: String): TreeMap[Array[Byte], Array[Byte]] = {
    val obs = Source.fromFile(obsLoc)
      .getLines
      .map( (line) => {
        val splits = line.split("::")
        Observation(splits(0).toLong, splits(1).toLong, splits(2).toDouble)
      })
      .toList
      .groupBy(_.user)
      .map({ case (user, obs) => {
          val obsMap: HashMap[Long, Double] = HashMap(obs.map(r => (r.data, r.score)):_*)
          (TachyonUtils.long2ByteArr(user), SerializationUtils.serialize(obsMap))
        }
      })
    val sortedObs = TreeMap(obs.toArray:_*)(ByteOrdering)
    sortedObs
  }
}

case class Observation(user: Long, data: Long, score: Double)

// Use tachyon sort ordering
object ByteOrdering extends Ordering[Array[Byte]] {
  def compare(a: Array[Byte], b: Array[Byte]) = TUtils.compare(a, b)
}




