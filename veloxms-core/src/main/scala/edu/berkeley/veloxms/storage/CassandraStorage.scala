package edu.berkeley.veloxms.storage

/**
 * A storage backend on top of Cassandra
 */

import com.datastax.driver.core.Cluster
import com.datastax.driver.core.querybuilder.{QueryBuilder, Insert}
import edu.berkeley.veloxms.util.Logging
import scala.util._
import scala.collection.immutable.HashMap
import edu.berkeley.veloxms._
import edu.berkeley.veloxms.util.KryoThreadLocal

// class CassandraStorage[U: ClassTag] (
class CassandraStorage[T, U] ( address: String,
                         keyspace: String,
                         table: String) extends ModelStorage[T, U] with Logging {

  // Set up the Cassandra cluster and session
  private val cluster = Cluster.builder().addContactPoint(address).build()
  private val session = cluster.connect()

  // Constants for column names
  private val Key = "key"
  private val Value = "value"

  // Make sure all tables exist and contain the necessary columns
  // TODO: If these fail, still have to make sure to close the cluster
  require {
    val cd = session.execute(s"SELECT * FROM $keyspace.$table LIMIT 1").getColumnDefinitions
    cd.contains(Key) && cd.contains(Value)
  }

  /**
   * Cleans up any necessary resources
   */
  override def stop() { cluster.close() }

  override def put(kv: (T, U)): Unit = {
    val kryo = KryoThreadLocal.kryoTL.get
    val k = kryo.serialize(kv._1)
    val v = kryo.serialize(kv._2)
    session.execute(QueryBuilder.insertInto(keyspace, table).value(Key, k).value(Value, v))
  }

  override def get(key: T): Option[U] = {
    try {
      val rawBytes = session.execute(s"SELECT * FROM $keyspace.$table WHERE $Key = $key")
          .all().get(0).getBytes(Value)
      val kryo = KryoThreadLocal.kryoTL.get
      val result = kryo.deserialize(rawBytes).asInstanceOf[U]
      Some(result)
    } catch {
      case u: Throwable => None
    }
  }
}



