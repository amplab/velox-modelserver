package edu.berkeley.veloxms.storage

import edu.berkeley.veloxms._
import edu.berkeley.veloxms.util.{Logging, EtcdClient, KryoThreadLocal}
import org.apache.spark.{SparkContext, SparkConf}
import sun.misc.{BASE64Decoder, BASE64Encoder}

import scala.collection.mutable
import scala.reflect.ClassTag
import scala.util.control.NonFatal

/**
 * A versioned broadcast that works via reading & writing to a global filesystem via the spark cluster
 * @tparam T Has ClassTag because sc.objectFile (to load the broadcast) requires a classtag
 */
class SparkVersionedBroadcast[T: ClassTag](sc: SparkContext, path: String) extends VersionedBroadcast[T] with Logging {
  private val cachedValues: mutable.Map[Version, T] = mutable.Map()

  override def put(value: T, version: Version): Unit = this.synchronized {
    sc.parallelize(Seq(value)).saveAsObjectFile(s"$path/$version")
  }

  override def get(version: Version): Option[T] = this.synchronized {
    val out = cachedValues.get(version).orElse(fetch(version))
    out.foreach(x => cachedValues.put(version, x))
    out
  }

  override def cache(version: Version): Unit = this.synchronized {
    fetch(version).foreach(x => cachedValues.put(version, x))
  }

  private def fetch(version: Version): Option[T] = {
    val location = s"$path/$version"
    try {
      Some(sc.objectFile(location).first())
    } catch {
      case NonFatal(e) =>
        logWarning(s"Failed to load broadcast. ${e.getMessage}")
        None
    }
  }
}