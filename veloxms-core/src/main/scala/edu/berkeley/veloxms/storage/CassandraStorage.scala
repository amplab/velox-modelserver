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
class CassandraStorage[T, U] ( address: String,
                         keyspace: String,
                         users: String,
                         items: String,
                         ratings: String,
                         val numFactors: Int) extends ModelStorage[T, U] with Logging {

  // Set up the Cassandra cluster and session
  private val cluster = Cluster.builder().addContactPoint(address).build()
  private val session = cluster.connect()

  // Constants for column names
  private val Key = "key"
  private val Value = "value"

  // Make sure all tables exist and contain the necessary columns
  // TODO: If these fail, still have to make sure to close the cluster
  require {
    val cd = session.execute(s"SELECT * FROM $keyspace.$users LIMIT 1").getColumnDefinitions
    cd.contains(Key) && cd.contains(Value)
  }
  require {
    val cd = session.execute(s"SELECT * FROM $keyspace.$items LIMIT 1").getColumnDefinitions
    cd.contains(Key) && cd.contains(Value)
  }
  require {
    val cd = session.execute(s"SELECT * FROM $keyspace.$ratings LIMIT 1").getColumnDefinitions
    cd.contains(Key) && cd.contains(Value)
  }

  def getFeatureData(context: T): Try[U] = {
    try {
      val rawBytes = session.execute(s"SELECT * FROM $keyspace.$items WHERE $Key = $context")
          .all().get(0).getBytes(Value)
      val kryo = KryoThreadLocal.kryoTL.get
      val array = kryo.deserialize(rawBytes).asInstanceOf[U]
      Success(array)
    } catch {
      case u: Throwable => Failure(u)
    }
  }

  def getUserFactors(userId: Long): Try[WeightVector] = {
    try {
      val rawBytes = session.execute(s"SELECT * FROM $keyspace.$users WHERE $Key = $userId")
          .all().get(0).getBytes(Value)
      val kryo = KryoThreadLocal.kryoTL.get
      val result = kryo.deserialize(rawBytes).asInstanceOf[WeightVector]
      Success(result)
    } catch {
      case u: Throwable => Failure(u)
    }
  }

  def getAllObservations(userId: Long): Try[Map[T, Double]] = {
    try {
      val rawBytes = session.execute(s"SELECT * FROM $keyspace.$ratings WHERE $Key = $userId")
          .all().get(0).getBytes(Value)
      val kryo = KryoThreadLocal.kryoTL.get
      val result = kryo.deserialize(rawBytes).asInstanceOf[HashMap[T, Double]]
      Success(result)
    } catch {
      case u: Throwable => Failure(u)
    }
  }

  def addScore(userId: Long, context: T, score: Double) = {
    throw new NotImplementedError()
  }

  /**
   * Cleans up any necessary resources
   */
  override def stop() { cluster.close() }
}



