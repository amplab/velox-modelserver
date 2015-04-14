package edu.berkeley.veloxms.storage

import edu.berkeley.veloxms._
import edu.berkeley.veloxms.util.{EtcdClient, KryoThreadLocal}

import scala.collection.mutable
import sun.misc.BASE64Encoder
import sun.misc.BASE64Decoder

/**
 * A simple in-etcd version of the versioned broadcast (Threadsafe)
 * TODO: Need cache invalidation when putting into prev version
 * TODO: Use Torrent for large files
 */
class VersionedEtcdBroadcast[T](id: String, etcdClient: EtcdClient) extends VersionedBroadcast[T] {
  private val cachedValues: mutable.Map[Version, T] = mutable.Map()
  private val encoder = new BASE64Encoder()
  private val decoder = new BASE64Decoder()
  override def put(value: T, version: Version): Unit = this.synchronized {
    val kryo = KryoThreadLocal.kryoTL.get
    val serVal = kryo.serialize(value)
    etcdClient.put(s"$id/$version", encoder.encode(serVal))
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
    val kryo = KryoThreadLocal.kryoTL.get
    etcdClient.get(s"$id/$version").map(decoder.decodeBufferToByteBuffer).map(kryo.deserialize).map(_.asInstanceOf[T])
  }
}