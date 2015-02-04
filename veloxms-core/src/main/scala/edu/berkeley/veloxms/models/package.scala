package edu.berkeley.veloxms
import java.util.concurrent.ConcurrentHashMap
import breeze.linalg._

package object models {

  type PartialResultsCache = ConcurrentHashMap[Long, (DenseMatrix[Double], DenseVector[Double])]

}
