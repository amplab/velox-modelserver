package edu.berkeley

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import scala.reflect._


package object veloxms {

  type FeatureVector = Array[Double]
  type WeightVector = Array[Double]
  type UserID = Long
  type Version = Long

  // TODO: Probably shouldn't be in the package object
  val jsonMapper = new ObjectMapper().registerModule(new DefaultScalaModule)
  def fromJson[T : ClassTag](node: JsonNode): T = {
    jsonMapper.treeToValue(node, classTag[T].runtimeClass.asInstanceOf[Class[T]])
  }

}
