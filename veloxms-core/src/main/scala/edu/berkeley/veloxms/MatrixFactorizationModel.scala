package edu.berkeley.veloxms

import edu.berkeley.veloxms.storage._
import com.typesafe.scalalogging._
import scala.util.Success
import scala.util.Failure
import scala.util.Try
import java.nio.ByteBuffer
import java.io.IOException

import tachyon.TachyonURI;
import tachyon.Pair;
import tachyon.r.sorted.ClientStore;
import tachyon.r.sorted.Utils;

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.Rating

class MatrixFactorizationModel(
    val numFeatures: Int,
    val modelStorage: ModelStorage[FeatureVector],
    val averageUser: WeightVector) extends Model[Long, FeatureVector]
    with LazyLogging {

  /**
   * User provided implementation for the given model. Will be called
   * by Velox on feature cache miss.
   */
  def computeFeatures(data: Long): FeatureVector = {
    modelStorage.getFeatureData(data) match {
      case Success(features) => features
      case Failure(thrown) => {
        val msg = "Error computing features: " + thrown
        logger.warn(msg)
        throw new Exception(msg)
      }
    }
  }

  def deserializeInput(data: Array[Byte]) : Long = {
    ByteBuffer.wrap(data).getLong

  }

  def getFeatures(item: Long, cache: FeatureCache[Long]): FeatureVector = {
    cache.getItem(item) match {
      case Some(f) => f
      case None => {
        val f = computeFeatures(item)
        cache.addItem(item, f)
        f
      }
    }
  }

  def getWeightVector(userId: Long) : WeightVector = {
    val result: Try[Array[Double]] = modelStorage.getUserFactors(userId)
    result match {
      case Success(u) => u
      case Failure(thrown) => {
        logger.warn("User weight not found: " + thrown)
        averageUser
      }
    }
  }

  def retrainInSpark(sparkMaster: String, trainingDataLoc: String) {


    // TODO finish implementing this method

    val numFeatures = 50
    val numIters = 20

    val sc = new SparkContext(sparkMaster, "RetrainVeloxModel")
    val data = sc.textFile(modelStorage.ratings)
    val ratings = data.map(_.split("::") match {
      case Array(user, item, score, date) => Rating(user.toInt, item.toInt, score.toDouble)
    })

    val model = ALS.train(ratings, 50, 20, 1)

    // model.userFeatures.mapPartitions( )//write to Tachyon)

    /*

    val userFeatures = model.userFeatures
    val userFeaturesFlat = userFeatures.map{case (a, b) => (a, b.mkString(","))}
    userFeaturesFlat.saveAsTextFile("userFeatures10M-r1.txt")

    val productFeatures = model.productFeatures
    val productFeaturesFlat = productFeatures.map{case (a, b) => (a, b.mkString(","))}
    productFeaturesFlat.saveAsTextFile("productFeatures10M-r1.txt")
    */

  }

}


object MatrixFactorizationModel {
  val lambda = 1



}



