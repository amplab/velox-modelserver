package edu.berkeley

import org.codehaus.jackson.map.ObjectMapper


package object veloxms {

  type FeatureVector = Array[Double]
  type WeightVector = Array[Double]
  type UserID = Long

  val jsonMapper = new ObjectMapper()
}
