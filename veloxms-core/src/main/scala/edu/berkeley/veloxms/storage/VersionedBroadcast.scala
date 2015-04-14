package edu.berkeley.veloxms.storage

import edu.berkeley.veloxms.Version

/**
 * Thread-safe versioned variable broadcasting
 */
trait VersionedBroadcast[T] {
  def get(version: Version): Option[T]
  def put(value: T, version: Version): Unit
  // Tries to fetch & cache the version. Any pre-existing cached value for the version will be invalidated
  def cache(version: Version): Unit
}
