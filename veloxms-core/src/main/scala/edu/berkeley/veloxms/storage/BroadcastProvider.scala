package edu.berkeley.veloxms.storage

import org.apache.spark.SparkContext

import scala.reflect.ClassTag

/**
 * This trait provides Versioned Broadcasts.
 * TODO: Add HTTP broadcast
 * TODO: Add Torrent broadcast
 */
trait BroadcastProvider {
  def get[T: ClassTag](id: String): VersionedBroadcast[T]
}

class SparkVersionedBroadcastProvider(sparkContext: SparkContext, path: String) extends BroadcastProvider {
  override def get[T: ClassTag](id: String): VersionedBroadcast[T] = new SparkVersionedBroadcast(sparkContext, s"$path/$id")
}