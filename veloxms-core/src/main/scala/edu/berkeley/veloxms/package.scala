package edu.berkeley

// import org.codehaus.jackson.map.ObjectMapper
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule


package object veloxms {

  type FeatureVector = Array[Double]
  type WeightVector = Array[Double]
  type UserID = Long

  val jsonMapper = new ObjectMapper().registerModule(new DefaultScalaModule)
}
