package edu.berkeley.veloxms.background

import java.util.concurrent._

import edu.berkeley.veloxms.util.Logging
import io.dropwizard.lifecycle.Managed

import scala.util.control.NonFatal

/**
 * This is the parent class for all tasks that need to execute periodically in the background
 * (not using servlet threads).
 * Just needs to be managed by the dropwizard environment lifecycle
 * after construction.
 *
 * @param delay  The delay between executions of this background task
 * @param unit  The time unit the delay is in
 */
abstract class BackgroundTask(delay: Long, unit: TimeUnit) extends Managed with Logging {
  // TODO: Should be using a some sort of shared threadpool for background tasks
  private val executor = Executors.newSingleThreadScheduledExecutor()

  /**
   * The task to execute periodically in the background.
   */
  protected def execute(): Unit

  private val task: Runnable = new Runnable {
    override def run(): Unit = {
      try {
        execute()
      } catch {
        case NonFatal(e) =>
          logWarning("Failed to execute background task", e)
        case e: InterruptedException =>
          logWarning("Background task was interrupted. Will no longer execute", e)
          throw e
        case e: Exception =>
          logError("Background task experienced a fatal error. Will no longer execute", e)
          throw e
      }
    }
  }

  override def start(): Unit = {
    executor.scheduleWithFixedDelay(task, delay, delay, unit)
  }

  override def stop(): Unit = {
    executor.shutdownNow()
  }
}
