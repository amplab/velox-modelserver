package edu.berkeley.veloxms.misc


import org.apache.commons.io.IOUtils
// import org.apache.commons.lang3.SerializationUtils
import tachyon.TachyonURI
import tachyon.Pair
import tachyon.r.sorted.ClientStore
import tachyon.r.sorted.{Utils => TUtils}
import edu.berkeley.veloxms.storage.TachyonUtils
import scala.collection.immutable.TreeMap
import scala.io.Source
import org.apache.spark.mllib.recommendation.Rating
// import com.typesafe.scalalogging._
import edu.berkeley.veloxms._
import com.massrelevance.dropwizard.scala.params.{LongParam, IntParam, BooleanParam}
import com.codahale.metrics.annotation.Timed
import javax.validation.Valid
import javax.ws.rs._
import javax.ws.rs.core.MediaType
import scala.collection.immutable.HashMap
import tachyon.r.sorted.ClientStore
import tachyon.TachyonURI
import java.nio.ByteBuffer
import java.io.ByteArrayOutputStream
import edu.berkeley.veloxms.util.{VeloxKryoRegistrar, KryoThreadLocal}

import edu.berkeley.veloxms.util.Logging
// import scala.pickling._
// import binary._

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

case class TestParams(tachloc: String, part: Int, create: Boolean, key: Long)

@Path("/misc/prep-tachyon")
@Consumes(Array(MediaType.APPLICATION_JSON))
@Produces(Array(MediaType.APPLICATION_JSON))
class WriteModelsResource extends Logging {


  // val logger = Logger(LoggerFactory.getLogger(classOf[WriteModelsResource]))

  @POST
  @Timed
  def writeAllToTachyon(@Valid locs: ModelLocations): Boolean = {
    logInfo("Writing items")
    writeMapToTachyon(readModel(locs.itemsSrc), locs.itemsDst, locs.partition)
    logInfo("Writing users")
    writeMapToTachyon(readModel(locs.usersSrc), locs.usersDst, locs.partition)
    logInfo("Writing observations")
    writeMapToTachyon(readObservations(locs.obsSrc), locs.obsDst, locs.partition, 3)
    logInfo("Finished prepping tachyon")
    true
  }

  // @POST
  // def testTachyon(@Valid params: TestParams): Boolean = {
  //     val store = if (params.create) {
  //       ClientStore.createStore(new TachyonURI(params.tachloc))
  //     } else {
  //       ClientStore.getStore(new TachyonURI(params.tachloc))
  //     }
  //     store.createPartition(params.part)
  //     val key = TachyonUtils.long2ByteArr(params.key)
  //     val arr: Array[Double] = Array(1.5, 2.5, 3.5, 4.5)
  //     val arrPickle = arr.pickle.value
  //
  //
  //     store.put(params.part, key, arrPickle)
  //     store.closePartition(params.part)
  //     logInfo(s"Wrote $key, $arrPickle to Tachyon partition ${params.part}")
  //     val rawBytes = store.get(key)
  //     logInfo(s"Found rawBytes: $rawBytes")
  //     val result = rawBytes.unpickle[Array[Double]]
  //     logInfo(s"Deserialized raw bytes to $result")
  //     true
  //
  //
  //
  //
  // }

  // Read MatrixFactorizationModel
  def readModel(modelLoc: String): TreeMap[Array[Byte], Array[Byte]] = {
    var i = 0
    val model = Source.fromFile(modelLoc).getLines.map( (line) => {
      val splits = line.split(",")
      val key = splits(0).toLong
      if (i < 10) logInfo(s"key: $key")
      i += 1
      val factors: Array[Double] = splits.slice(1, splits.size).map(_.toDouble)
      // TODO Find less brittle way to allocate bytebuffer sizes
      val buffer = ByteBuffer.allocate(factors.size*8*2)
      val kryo = KryoThreadLocal.kryoTL.get
      val result = kryo.serialize(factors, buffer).array
      (TachyonUtils.long2ByteArr(key), result)
    })

    val sortedModel = TreeMap(model.toArray:_*)(ByteOrdering)
    sortedModel
  }

  def writeMapToTachyon(map: TreeMap[Array[Byte], Array[Byte]], loc: String,
      partition: Int = 0, splits: Int = 1) {
    val store = ClientStore.createStore(new TachyonURI(loc))
    for (pa <- 0 to splits) {
      store.createPartition(partition + pa)
    }
    val splitPoint: Int = map.size / splits
    var i = 0
    val keys = map.keys.toList
    while (i < map.size) {
      val k = keys(i)
      val v = map(k)
      val p = partition + (i / splitPoint)
      store.put(p, k, v)
      i += 1
    }
    // map.foreach( {case (key, value) => store.put(partition, key, value) })

    for (pa <- 0 to splits) {
      store.closePartition(partition + pa)
    }
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
          val kryo = KryoThreadLocal.kryoTL.get
          val buffer = ByteBuffer.allocate(obsMap.size*8*8*2)
          val serMap = kryo.serialize(obsMap, buffer).array
          (TachyonUtils.long2ByteArr(user), serMap)
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




