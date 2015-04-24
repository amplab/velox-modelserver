package edu.berkeley.veloxms.util


// Lightly adapted from:
// https://github.com/nikore/scala-etcd/blob/343e935b7a28b88a058a4d09c2c40863ee2e32bf/src/main/scala/net/nikore/etcd/EtcdJsonProtocol.scala

// Single keys
case class EtcdResponse(action: String, node: NodeResponse, prevNode: Option[NodeResponse])

case class NodeResponse(
  createdIndex: Int,
  key: String,
  modifiedIndex: Int,
  value: Option[String],
  expiration: Option[String],
  ttl: Option[Int]
)


// Directories
case class EtcdListResponse(action: String, node: NodeListElement)

case class NodeListElement(
  key: String,
  dir: Option[Boolean],
  value: Option[String],
  nodes: Option[List[NodeListElement]],
  createdIndex: Int,
  modifiedIndex: Int,
  expiration: Option[String],
  ttl: Option[Int]
)

  
case class EtcdError(cause: String, errorCode: Int, index: Int, message: String)
