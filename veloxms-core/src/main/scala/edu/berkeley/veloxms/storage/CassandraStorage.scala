package edu.berkeley.veloxms.storage

/**
 * A storage backend on top of Cassandra
 */

import com.datastax.driver.core.Cluster
import edu.berkeley.veloxms.util.Logging
import scala.util._
import scala.collection.immutable.HashMap
import edu.berkeley.veloxms._
import edu.berkeley.veloxms.util.KryoThreadLocal

// class CassandraStorage[U: ClassTag] (
class CassandraStorage ( address: String,
                         keyspace: String,
                         usersCF: String,
                         itemsCF: String,
                         ratingsCF: String,
                         val numFactors: Int) extends ModelStorage[FeatureVector] with Logging {

  // Set up the Cassandra cluster and session
  private val cluster = Cluster.builder().addContactPoint(address).build()
  private val session = cluster.connect(keyspace)

  // Constants for column names
  private val Key = "key"
  private val Value = "value"

  // Make sure all tables exist and contain the necessary columns
  require(session.execute("SELECT * FROM " + usersCF + " LIMIT 1").getColumnDefinitions.contains(Key))
  require(session.execute("SELECT * FROM " + usersCF + " LIMIT 1").getColumnDefinitions.contains(Value))
  require(session.execute("SELECT * FROM " + itemsCF + " LIMIT 1").getColumnDefinitions.contains(Key))
  require(session.execute("SELECT * FROM " + itemsCF + " LIMIT 1").getColumnDefinitions.contains(Value))
  require(session.execute("SELECT * FROM " + ratingsCF + " LIMIT 1").getColumnDefinitions.contains(Key))
  require(session.execute("SELECT * FROM " + ratingsCF + " LIMIT 1").getColumnDefinitions.contains(Value))

  def getFeatureData(itemId: Long): Try[FeatureVector] = {
    try {
      val rawBytes = session.execute("SELECT * FROM " + itemsCF + " WHERE " + Key + " = " + itemId).all().get(0).getBytes(Value)
      val kryo = KryoThreadLocal.kryoTL.get
      val array = kryo.deserialize(rawBytes).asInstanceOf[FeatureVector]
      Success(array)
    } catch {
      case u: Throwable => Failure(u)
    }
  }

  def getUserFactors(userId: Long): Try[WeightVector] = {
    try {
      val rawBytes = session.execute("SELECT * FROM " + usersCF + " WHERE " + Key + " = " + userId).all().get(0).getBytes(Value)
      val kryo = KryoThreadLocal.kryoTL.get
      val result = kryo.deserialize(rawBytes).asInstanceOf[WeightVector]
      Success(result)
    } catch {
      case u: Throwable => Failure(u)
    }
  }

  def getAllObservations(userId: Long): Try[Map[Long, Double]] = {
    try {
      val rawBytes = session.execute("SELECT * FROM " + ratingsCF + " WHERE " + Key + " = " + userId).all().get(0).getBytes(Value)
      val kryo = KryoThreadLocal.kryoTL.get
      val result = kryo.deserialize(rawBytes).asInstanceOf[HashMap[Long, Double]]
      Success(result)
    } catch {
      case u: Throwable => Failure(u)
    }
  }

  /**
   * Cleans up any necessary resources
   */
  override def close() { cluster.close() }
}



