package edu.berkeley.veloxms.background

import java.util.concurrent._

import edu.berkeley.veloxms.util.Logging
import io.dropwizard.lifecycle.Managed

import scala.util.control.NonFatal

abstract class BackgroundTask(delay: Long, unit: TimeUnit) extends Managed with Logging {
  // TODO: Should be using a some sort of shared threadpool for background tasks
  private val executor = Executors.newSingleThreadScheduledExecutor()

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
