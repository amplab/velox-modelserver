package edu.berkeley.veloxms.background

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{ConcurrentLinkedQueue, TimeUnit}

import com.codahale.metrics.Timer
import edu.berkeley.veloxms._
import edu.berkeley.veloxms.models.Model

import scala.collection.mutable

class OnlineUpdateManager[T](model: Model[T], delay: Long, unit: TimeUnit, timer: Timer) extends BackgroundTask(delay, unit) {
  private val onlineUpdatesEnabled: AtomicBoolean = new AtomicBoolean(true)
  private val observations = new ConcurrentLinkedQueue[(UserID, T, Double)]()

  def enableOnlineUpdates(): Unit = onlineUpdatesEnabled.set(true)
  def disableOnlineUpdates(): Unit = onlineUpdatesEnabled.set(false)

  def addObservation(userID: UserID, context: T, score: Double): Unit = {
    observations.add((userID, context, score))
  }

  override protected def execute(): Unit = synchronized {
    if (onlineUpdatesEnabled.get()) {
      val timeContext = timer.time()
      try {
        val currentVersion = model.currentVersion
        val uidsToUpdate = mutable.Set[UserID]()
        (0 until observations.size()).foreach(_ => {
          val (uid, context, score) = observations.poll()
          uidsToUpdate += uid
          model.addObservation(uid, context, score)
        })

        model.updateUsers(uidsToUpdate, currentVersion)
      } finally {
        timeContext.stop()
      }
    }
  }
}
