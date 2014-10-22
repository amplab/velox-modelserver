package edu.berkeley.veloxms.client

// import breeze.stats.distributions.{Gaussian, Uniform => BUniform}
import edu.berkeley.veloxms.util.Logging
import scala.util.Random
import scala.collection.mutable

// TODO(crankshaw) this code is a mess right now


/**
 * Classes should extend this trait to create model-specific
 * requests.
 */
// trait RequestGenerator {
//
//
//
// }

object RequestDist extends Enumeration {
  type RequestDist = Value
  val Uniform, Normal, Zipfian, SkewedLatest = Value
}

// TOOD change userId to user, itemId to item when I change it in
// AddObservationResource.scala
case class ObserveRequest(userId: Long, itemId: Long, score: Double)
case class PredictRequest(user: Long, item: Long)

import RequestDist._

// generate
class MFRequestor (
    numUsers: Long = 100,
    numItems: Long = 1000,
    /* The percentage of requests that are adding observations */
    // percentObs: Double = 0.2,
    percentObs: Double = -1.0,
    userDist: RequestDist = Uniform,
    itemDist: RequestDist = Uniform,
    maxScore: Double = 10.0,
    modelSize: Int = 50) extends Logging {


  val reqTypeRand = new Random
  val scoreRand = new Random
  
  // TODO(crankshaw) add other types of samplers
  val userSampler: PopSampler = userDist match {
    case Uniform => new UniformSampler(numUsers)
    case sa: PopSampler => {
      logWarning(s"$sa sampler currently unsupported, defaulting to Uniform")
      new UniformSampler(numUsers)
    }
  }

  def pickUser(): Long = {
    userSampler.nextLong()
  }


  val itemPredictionSampler: PopSampler = itemDist match {
    case Uniform => new UniformSampler(numItems)
    case sa: PopSampler => {
      logWarning(s"$sa sampler currently unsupported, defaulting to Uniform")
      new UniformSampler(numItems)
    }
  }

  // TODO(crankshaw) this should be sampling without replacement per user
  val itemObservationSampler = new SamplerWithoutReplacement(numItems)
    // new SamplerWithoutReplacement(numItems)



  def pickItemPredict(): Long = {
    itemPredictionSampler.nextLong()
  }

  def pickItemObserve(user: Long): Option[Long] = {
    // TODO(crankshaw) handle running out of items for that user
    itemObservationSampler.nextItem(user)
  }

  def getRequest: Either[ObserveRequest,PredictRequest] = {
    var user = pickUser()
    if (reqTypeRand.nextDouble() < percentObs) {
      var item = pickItemObserve(user)
      // have to loop because when doing sampling without replacement we could run out of
      // items for a particular user to observe
      while (item.isEmpty) {
        user = pickUser()
        item = pickItemObserve(user)
      }
      Left(ObserveRequest(user, item.get, scoreRand.nextDouble()*maxScore))
    } else {
      Right(PredictRequest(user, pickItemPredict()))
    }

  }

}

trait PopSampler { def nextLong(): Long }

/**
 * ub the exclusive upper bound of values being samples.
 *    The sampler will sample integers between [0, ub)
 */
class UniformSampler(ub: Long) extends PopSampler {
  val rand = new Random()

  def nextLong(): Long = {
    rand.nextInt(ub.toInt).toLong
  }
}

class SamplerWithoutReplacement(maxItem: Long) {
  val rand = new Random()

  // Keeps track of random order to add observations for items for each user
  // Used to do sampling without replacement on item observations
  val itemRequestHistory = new mutable.HashMap[Long, Iterator[Long]]
  def nextItem(user: Long): Option[Long] = {
    val iter = itemRequestHistory.get(user).getOrElse({
      val newIter = rand.shuffle((0L until maxItem).toIterator)
      itemRequestHistory.put(user, newIter)
      newIter
    })
    if (iter.hasNext) {
      Some(iter.next)
    } else {
      None
    }
  }
}

